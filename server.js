const { spawn } = require("child_process");
const express = require("express");
const { mkdirSync, readdirSync } = require("fs");
const multer = require("multer");
const path = require("path")
const app = express();

mkdirSync("static/imports/", {recursive: true})
mkdirSync("static/exports/", {recursive: true})

const IDLE_STATE = "IDLE";
const BUSY_STATE = "BUSY";

let state = IDLE_STATE;
let lastResult = {
    start: new Date(),
    importExitCode: 0,
    importStdout: "",
    importStderr: "",
    exportExitCode: 0,
    exportStdout: "",
    exportStderr: "",
    end: new Date()
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

app.get("/state", (req, res) => res.json({state, lastResult}));

app.get("/files", (req, res) => {
    res.json({
        exported: readdirSync("static/exports/"),
        imported: readdirSync("static/imports/")
    })
})

app.post("/import", upload.single("file"), (req, res) => {
    if( state != IDLE_STATE ) res.status(401).end("SERVER IS BUSY. SEE <a href='./state'>CURRENT STATE</a>")
    res.end("Running");
    state = BUSY_STATE;
    let start = new Date();
    let importProc = spawn("env/bin/python",["import.py","jurisprudencia.7.0","-f",req.file.path]);
    let importProcStdout = "";
    let importProcStderr = "";
    importProc.stdout.on("data",data => importProcStdout+=data.toString())
    importProc.stderr.on("data",data => importProcStderr+=data.toString())
    importProc.on("error", () => {});
    importProc.on("close", () => {
        lastResult.importExitCode = importProc.exitCode
        lastResult.importStderr = importProcStderr
        lastResult.importStdout = importProcStdout

        let exportProc = spawn("env/bin/python",["export-with-original.py","jurisprudencia.7.0","-e","UUID","-e","CONTENT","-e","Sumário","-e","Texto","-e","URL","-e","Tipo","-e","Processo","-i","UUID","-o","static/exports/"]);
        let exportProcStdout = "";
        let exportProcStderr = "";
        exportProc.stdout.on("data",data => exportProcStdout+=data.toString())
        exportProc.stderr.on("data",data => exportProcStderr+=data.toString())
        importProc.on("error", () => {})
        exportProc.on("close", () => {
            lastResult.exportExitCode = exportProc.exitCode;
            lastResult.exportStdout = exportProcStderr;
            lastResult.exportStderr = exportProcStdout;
            lastResult.end = new Date()
            state = IDLE_STATE;
        })
    })
})

app.listen(8000);