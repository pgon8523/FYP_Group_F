module.exports = app => {
    const backend = require('../controller/controller.js');
    var router = require("express").Router();
    router.get("/clean", backend.clean);
    router.get("/runModel/:selectData-:selectQuery-:selectQueryNum", backend.runModel);
    router.get("/retrainModel/:selectData", backend.retrainModel);
    router.post("/image", backend.image);
    router.post("/ratio", backend.queryGeneration);
    router.post("/planTree", backend.planTree);
    app.use('/api', router);
};
