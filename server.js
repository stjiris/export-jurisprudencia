const { spawn, spawnSync } = require("child_process");
const express = require("express");
const { mkdirSync, readdirSync, rmSync, writeFileSync } = require("fs");
const multer = require("multer");
const path = require("path")
const app = express();
const {Client} = require('@elastic/elasticsearch');
const client = new Client({node: process.env.ES_URL || 'http://localhost:9200'});

const CURRENT_INDEX = "jurisprudencia.9.4";

mkdirSync("static/imports/", {recursive: true})
mkdirSync("static/exports/", {recursive: true})
mkdirSync("static/results/", {recursive: true})

const IDLE_STATE = "IDLE";
const BUSY_STATE = "BUSY";

let state = IDLE_STATE;
let lastResult = {
    importStart: new Date(),
    importExitCode: 0,
    importStdout: "",
    importStderr: "",
    importEnd: new Date(),
    exportStart: new Date(),
    exportExitCode: 0,
    exportStdout: "",
    exportStderr: "",
    exportEnd: new Date()
}

var storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, 'static/imports/')
    },
    filename: function (req, file, cb) {
        cb(null, Date.now() + path.extname(file.originalname)) //Appending extension
    }
  })

const upload = multer({dest: "static/imports/", storage});

app.set("json spaces", 2)

app.use(express.static("static"));

app.get("/fields", (req, res) => {
    let r = spawnSync("env/bin/python",["fields.py"]);
    res.write(r.stdout.toString())
    res.end()
})

app.get("/field-info", (req, res) => {
    client.get({
        index: "terms-info.0.0",
        id: req.query.term
    }).then( r => {
        res.send(r._source.text)
    }).catch(e => {
        res.status(404).send()
    })
})
app.post("/field-info", express.urlencoded({extended: true}), (req, res) => {
    if( req.body.code != process.env.CODE ){
        return res.status(400).end("CODE IS INCORRECT");
    }
    client.index({
        index: "terms-info.0.0",
        id: req.body.term,
        document: {
            text: req.body.text
        }
    }).then( r => {
        res.send("OK")
    }).catch(e => {
        res.status(500).send(e.toString())
    })
})

app.get("/search", async (req, res) => {
    let iuec = req.query.iuec;
    if( !iuec ) return res.json(null);

    let found = await client.get({
        index: CURRENT_INDEX,
        id: iuec
    }).catch( e => null);
    if( found ) return res.json(found)

    
    let r = await client.search({
        index: CURRENT_INDEX,
        query: {
            term: {
                UUID: iuec
            }
        }
    })
    if( r.hits.hits.length == 0 ){
        r = await client.search({
            index: CURRENT_INDEX,
            query: {
                term: {
                    ECLI: iuec
                }
            }
        })
        if( r.hits.hits.length == 0 ){
            return res.json(null);
        }
    }
    return res.json(r.hits.hits[0])
})

app.post("/update", express.json(),(req, res) => {
    if( req.body.code != process.env.CODE ){
        return res.status(400).end("CODE IS INCORRECT");
    }
    client.update({
        index: CURRENT_INDEX,
        doc: req.body.doc,
        id: req.body.id
    }).then( r => res.json(r) ).catch( e => {console.log(e); res.json("Error")})
})

app.get("/state", (req, res) => res.json({state, lastResult}));

app.get("/files", (req, res) => {
    res.json({
        exported: readdirSync("static/exports/"),
        imported: readdirSync("static/imports/"),
        results: readdirSync("static/results/")
    })
})

app.post("/export", upload.none(), (req, res) => {
    if( state != IDLE_STATE ){
        return res.status(401).end("SERVER IS BUSY. SEE <a href='./state.html'>CURRENT STATE</a>")
    }
    if( req.body.code != process.env.CODE ){
        return res.status(400).end("CODE IS INCORRECT");
    }
    state = BUSY_STATE;
    res.end("Running");
    let filename = Date.now();
    writeFileSync(`static/imports/${filename}.txt`, `Pedido de exportação evefetuado em ${new Date(filename)}`)
    lastResult.importStart = new Date(),
    lastResult.importExitCode = 0,
    lastResult.importStdout = "",
    lastResult.importStderr = "",
    lastResult.importEnd = new Date(),
    lastResult.exportStart = new Date()
    let fields = Array.isArray(req.body.field) ? req.body.field : [req.body.field]  || []
    let exportProc = spawn("env/bin/python",["export-with-original.py",CURRENT_INDEX,"-i","UUID","-o","static/exports/","-n",filename,"-a",...fields.flatMap(o => ["-e", o])]);
    let exportProcStdout = "";
    let exportProcStderr = "";
    exportProc.stdout.on("data",data => exportProcStdout+=data.toString())
    exportProc.stderr.on("data",data => exportProcStderr+=data.toString())
    exportProc.on("error", () => {})
    exportProc.on("close", () => {
        lastResult.exportExitCode = exportProc.exitCode;
        lastResult.exportStdout = exportProcStderr;
        lastResult.exportStderr = exportProcStdout;
        lastResult.exportEnd = new Date()
        writeFileSync(`static/results/result-${filename}.json`, JSON.stringify(lastResult));
        state = IDLE_STATE;
    })
})

app.post("/import", upload.single("file"), (req, res) => {
    if( state != IDLE_STATE ){
        rmSync(req.file.path)
        return res.status(401).end("SERVER IS BUSY. SEE <a href='./state.html'>CURRENT STATE</a>")
    }
    if( req.body.code != process.env.CODE ){
        rmSync(req.file.path)
        return res.status(400).end("CODE IS INCORRECT");
    }
    let filename = path.basename(req.file.path,".xlsx")
    res.end("Running");
    state = BUSY_STATE;
    lastResult.importStart = new Date()
    let importProc = spawn("env/bin/python",["import.py",CURRENT_INDEX,"-f",req.file.path]);
    let importProcStdout = "";
    let importProcStderr = "";
    importProc.stdout.on("data",data => importProcStdout+=data.toString())
    importProc.stderr.on("data",data => importProcStderr+=data.toString())
    importProc.on("error", () => {});
    importProc.on("close", () => {
        lastResult.importExitCode = importProc.exitCode
        lastResult.importStderr = importProcStderr
        lastResult.importStdout = importProcStdout
        lastResult.importEnd = new Date()
        lastResult.exportStart = new Date()
        let exportProc = spawn("env/bin/python",["export-with-original.py",CURRENT_INDEX,"-i","UUID","-o","static/exports/","-n",filename,"-x","Número de Processo","-x","ECLI","-x","Tribunal de Recurso - Processo","-x","URL"]);
        let exportProcStdout = "";
        let exportProcStderr = "";
        exportProc.stdout.on("data",data => exportProcStdout+=data.toString())
        exportProc.stderr.on("data",data => exportProcStderr+=data.toString())
        exportProc.on("error", () => {})
        exportProc.on("close", () => {
            lastResult.exportExitCode = exportProc.exitCode;
            lastResult.exportStdout = exportProcStderr;
            lastResult.exportStderr = exportProcStdout;
            lastResult.exportEnd = new Date()
            writeFileSync(`static/results/result-${filename}.json`, JSON.stringify(lastResult));
            state = IDLE_STATE;
        })
    })
})

app.listen(9999, "127.0.0.1");