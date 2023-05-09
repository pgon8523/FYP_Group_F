const { stderr } = require("process");
const backend = require("../models/model.js")
cfg = backend.cmdConfig

exports.clean = (req, res) => {
    console.log("Starting function clean.");
    // const {spawnSync, spawnSync} = require('child_process');
    const {spawnSync} = require('child_process');
	var ls = ""
    console.log([ cfg["path"] + "/" +  cfg["cmdClean"], cfg["path"] , cfg["data1"] ]);
    ls = spawnSync('bash', [ cfg["path"] + "/" +  cfg["cmdClean"], cfg["path"] , cfg["data1"] ]);
    console.log('stdout: ' + ls.stdout);
    console.log('stderr: ' + ls.stderr);
    console.log('exit code: ' + ls.status);

    ls = spawnSync('bash', [ cfg["path"] + "/" +  cfg["cmdClean"], cfg["path"] , cfg["data2"] ]);
    console.log('stdout: ' + ls.stdout);
    console.log('stderr: ' + ls.stderr);
    console.log('exit code: ' + ls.status);

    ls = spawnSync('bash', [ cfg["path"] + "/" +  cfg["cmdClean"], cfg["path"] , cfg["data3"] ]);
    console.log('stdout: ' + ls.stdout);
    console.log('stderr: ' + ls.stderr);
    console.log('exit code: ' + ls.status);

    ls = spawnSync('bash', [ cfg["path"] + "/" +  cfg["cmdClean"], cfg["img_absolute_path"] , cfg["data4"] ]);
    console.log('stdout: ' + ls.stdout);
    console.log('stderr: ' + ls.stderr);
    console.log('exit code: ' + ls.status);

}

// exports.run4 = (req, res) => {
//     // Image data is a little bit different. You can not simply use command "cat data4.png" to show the content of the data. 
//     // Instead, you should place the image file to the propoer folder (/home/fypgf/gui/web/express/app/views/img) and return a url "http://137.189.59.166:8000/img/data4.png"
//     // Read the script "run4.sh" and the following lines carefully for further processing.
//     console.log("Starting function run4.(Showing img!)");
//     const {spawnSync} = require('child_process');
//     // console.log(`[ cfg["path"] + "/" + cfg["cmd4"], cfg["path"]`)
//     const ls = spawnSync('bash', [ cfg["path"] + "/" + cfg["cmd4"], cfg["path"], cfg["img_absolute_path"],cfg["data4"]]);
//     console.log(cfg["path"] + "/" + cfg["cmd4"])
//     console.log(cfg["img_absolute_path"])
//     console.log('stdout: ' + ls.stdout);
//     console.log('stderr: ' + ls.stderr);
//     console.log('exit code: ' + ls.status);

//     res.send("http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + cfg["data4"])
// };

// exports.runpg = (req, res) => {
//     const {spawnSync} = require('child_process');
//     // command:
//     // bash /home/fypgf/gui/web/cmd/runpg.sh /home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/join_title_backup explain_join_query_3.txt.sql log_join_3.txt
//     const ls = spawnSync('bash', [ cfg["path"] + "/" + "runpg.sh", "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/join_title_backup", "explain_join_query_3.txt.sql", "log_join_3.txt"]);

//     // bash /home/fypgf/gui/web/cmd/parse.sh /home/fypgf/gui/web/cmd/parse.py /home/fypgf/gui/web/cmd/log_join_3.txt
//     const ls2 = spawnSync('bash', [cfg["path"] + "/" + "parse.sh", cfg["path"] + "/" + "parse.py", "/home/fypgf/gui/web/cmd/log_join_3.txt"]);
//     res.send(ls2.stdout)
// }

exports.retrainModel = (req, res) => {
    const {spawnSync} = require('child_process');

    // should be form in { selectData: 'IMDB', selectQuery: '2', selectQueryNum: '10' }
    let pg_dataset = "";
    console.log("retraining the model ")
    console.log(req.params)

    // switching filepath according to the selected schema name
    if (req.params.selectData == "IMDB") {
        schema = 'imdb'
        trainingpath = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_pure_training";
        datapath = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean"
        pg_dataset = "kf_job"
        dataset_to_show = "IMDB"

    } else {
        schema = 'tpcds'
        trainingpath = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean/join_store_sales_store_item_customer_promotion_10_data_centric_427";

        datapath = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/TPCDS_clean";
        pg_dataset = "kf_tpcds"
        // console.log("exec filepath: " + filepath)
    }

    // starting server for retraining the model, but need to wait until training process is done
    console.log( ["running script path:" + cfg["path"] + '/runserver.sh', `--schema=${schema}`, `--datapath=${datapath}`, `--querypath=${trainingpath}`]);

    const startServer = spawnSync('bash', [cfg['path'] + '/runserver.sh', `--schema=${schema}`, `--datapath=${datapath}`, `--querypath=${trainingpath}`]);
    console.log("startServer stderr: ", startServer.stderr.toString('ascii'))
    console.log("startServer stdout: ", startServer.stdout.toString('ascii'))
    console.log("startServer exit code:", startServer.status)

    res.send(startServer.stdout)
}


exports.runModel = (req, res) => {
    const {spawnSync} = require('child_process');

    // should be form in { selectData: 'IMDB', selectQuery: '2', selectQueryNum: '10' }
    console.log(req.params); 

    let filepath = "";
    let pg_dataset = "";

    // switching filepath according to the selected schema name
    if (req.params.selectData == "IMDB") {
        trainingpath = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_pure_training";
        datapath = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean"
        pg_dataset = "kf_job"
        dataset_to_show = "IMDB"

    } else {
        // trainingpath = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/imdb_clean/824_pure_training";
        datapath = "/home/fypgf/hslee0824/FYP-NNGP-PG/NNGP_revision/SQL_Server_card/tpcds_clean"
        pg_dataset = "kf_tpcds"
        // console.log("exec filepath: " + filepath)
    }

    // where the query will be stored 
    filepath="/home/fypgf/gui/web/cmd/train_test_ratio/test_for_saving_train_test";
    // no. of table to be join
    let queryString = req.params.selectQuery; 
    // no.of queries
    let queryNum = req.params.selectQueryNum; 

    // input file path: sql file to be exec
    let input_filepath = `explain_join_query_${queryString}.txt.sql`
    console.log("input: " + input_filepath)
    // output file path: logging data of running query with/without running model
    let output_filepath = `log_join_${queryString}.txt`
    console.log("output:" + output_filepath)

    // shuffle sql file for randomess and store into temp.sql file
    const shuf = spawnSync('bash', [ cfg["path"] + "/shuf.sh", queryNum, `${filepath}/${input_filepath}`, `${filepath}/temp.sql`]);
    console.log("\nshuf stderr: ", shuf.stderr.toString('ascii'))
    console.log("\nshuf exit code: ", shuf.status)

    // clearing image in previous running
    // const clear_image = spawnSync('bash', [cfg["path"] + "/clear_image.sh"])
    // console.log("clear_image stderr: " + clear_image.stderr.toString('ascii'));

    // reord req.body into json file for q-error 
    const recordError = spawnSync('bash', [cfg["path"] + "/recordBody.sh", JSON.stringify(req.params), "/home/fypgf/gui/web/cmd/error.json"]);
    console.log("recordError  stderr: ", recordError.stderr.toString('ascii'))
    console.log("recordError exit code: ", recordError.status)
    console.log( ["running script path:" + cfg["path"] + "/runpg.sh", `${pg_dataset}`, `${filepath}/temp.sql`, cfg["path"] + `/${output_filepath}`])

    // starting server for retraining the model, but need to wait until training process is done
    // const startServer = spawnSync('bash', [cfg['path'] + '/runserver.sh', `--datapath=${datapath}`, `--querypath=${trainingpath}`]);
    // console.log("startServer stderr: ", startServer.stderr.toString('ascii'))
    // console.log("startServer exit code:", startServer.status)
    // console.log( ["running script path:" + cfg["path"] + '/runserver.sh', `--datapath=${datapath}`, `--querypath=${trainingpath}`]);

    // Stop the nngp model
    console.log("\nstop nngp ")
    const start = spawnSync('bash', [ cfg["path"] + "/stopnngp.sh"])
    console.log("stop nngp stderr: ", start.stderr.toString('ascii'))
    console.log("stop nngp stdout: ", start.stdout.toString('ascii'))
    console.log("stop nngp exit code = ", start.status)

    // Run the sql query with postgresql planner 
    //  $1: (pg_dataset)kf_xxx , $2: (filepath/temp.sql), $3: (/output_filepath)
    const run1 = spawnSync('bash', [ cfg["path"] + "/runpg.sh", `${pg_dataset}`, `${filepath}/temp.sql`, cfg["path"] + `/${output_filepath}`]);
    console.log("postgresql planner run stderr: ", run1.stderr.toString('ascii'))
    console.log("planner stdout: ", run1.stdout.toString('ascii'))
    console.log("planner exit code = ", run1.status)

    console.log("\nstart nngp ")
    const stop = spawnSync('bash', [ cfg["path"] + "/startnngp.sh"])
    console.log("start nngp std err: ", stop.stderr.toString('ascii'))
    console.log("start nngp stdout: ", stop.stdout.toString('ascii'))
    console.log("start nngp exit code = ", stop.status)

    // run query with nngp model 
    const run2 = spawnSync('bash', [ cfg["path"] + "/runpg.sh", `${pg_dataset}`, `${filepath}/temp.sql`, cfg["path"] + `/nngp_${output_filepath}`]);
    console.log("\nnngp run stderr: ", run2.stderr.toString('ascii'))
    console.log("nngp stdout: ", run2.stdout.toString('ascii')) 
    // console.log("run2 exit code = ", run2.status)

    // bash parse.sh parse.py (read 3 files, print the json data)
    // f1: planning time and execution time (postgres result)
    // f2: temporary queries
    // f3: (nngp result)
    const ls2 = spawnSync('bash', [cfg["path"] + "/parse.sh", cfg["path"] + "/parse.py", cfg["path"] + `/${output_filepath}`, `${filepath}` + "/temp.sql", cfg["path"] + `/nngp_${output_filepath}` ]);
    console.log("stderr: ", ls2.stderr.toString('ascii'))
    console.log("\nend of running model")
    res.send(ls2.stdout)
}

exports.image = async (req, res) => {
    const {spawnSync} = require('child_process');

    // req.body: plan, execution, nngp_plan, nngp_execution
    console.log(req.body);

    // write comparsion data into testing.json file  
    const record = spawnSync('bash', [cfg["path"] + "/recordBody.sh", JSON.stringify(req.body), cfg["path"] + "/testing.json"]);
    console.log("record data for image generation stderr: " + record.stderr.toString('ascii'));
    console.log("recod stdout: " + record.stdout.toString('ascii'));
    console.log("record exit code = ", record.status);
    
    // clearing images in the express/views
    // const clear_image = await spawnSync('bash', [cfg["path"] + "/clear_image.sh"])
    // console.log("clear_image stderr: " + clear_image.stderr.toString('ascii'));
    // console.log("clear_image stdout: " + clear_image.stdout.toString('ascii'));
    // console.log("clear_image exit code = ", clear_image.status);

    // exectue image generation function
    // output file path must be /home/fypgf/gui/web/express/app/views for rendering in web page 
    const image = await spawnSync('bash', [cfg["path"] + "/plot_graph.sh"])
    console.log("image ploying stderr: " + image.stderr.toString('ascii'));
    console.log("image stdout: " + image.stdout.toString('ascii'));
    console.log("image exit code = ", image.status);
    console.log("end of generating image \n")


    const q_error =  await spawnSync('bash', [cfg["path"] + "/q_error.sh"])
    console.log("q_error image stderr: " + q_error.stderr.toString('ascii'));
    console.log("q_error stdout: " + q_error.stdout.toString('ascii'));
    console.log("q_error exit code = ", q_error.status);
    console.log("end of generating q_error \n")

    // return image for rendering in web 
    
    res.send({
        average: "http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + "AVG_Comparison.png",
        execution: "http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + "Execution_Time_Comparison.png",
        planning: "http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + "Planning_Time_Comparison.png",
        high_exec: "http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + "Execution_Time_Comp_High.png",
        low_exec: "http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + "Execution_Time_Comp_Low.png",
        high_plan: "http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + "Planning_Time_Comp_High.png",
        low_plan: "http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + "Planning_Time_Comp_Low.png",
        q_error: "http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + "Q_error_between_PostgreSQL_and_NNGP.png",
    });
}

exports.queryGeneration = async (req, res) => {
    const {spawnSync} = require('child_process');
    // req.body: plan, execution, nngp_plan, nngp_execution
    /**
     * record following data for python code, train_test_information
     * {
     * "relation_name": ["imdb"],
        "train_test_ratio": { "train": ["0.8"], "test" : ["0.2"] },
        "non_join_query_ratio" : { "join": ["0.7"], "non_join" : ["0.3"] },
        "min_max" : { "min": ["3"], "max": ["5"] }
        }
     */
    // record the req body into json file for generating query
    console.log(req.body)
    const record = spawnSync('bash', [cfg["path"] + "/recordBody.sh", JSON.stringify(req.body), "/home/fypgf/gui/web/cmd/train_test_ratio/train_test_information.json"]);
    console.log("record train_test info stderr: " + record.stderr.toString('ascii'));
    console.log("record stdout: " + record.stdout.toString('ascii'));
    console.log("record exit code = ", record.status);

    // generating query according to the json file
    console.log("running: " + [cfg["path"] + "/generate.sh"] )
    const generate = spawnSync('bash', [cfg["path"] +  "/generate.sh"]);
    console.log("gernerate query stderr: " + generate.stderr.toString('ascii'));
    console.log("gen stdout: " + generate.stdout.toString('ascii'));
    console.log("gen exit code = ", generate.status);

    res.send({message: generate.stdout.toString('ascii') });
}

exports.planTree = async (req, res) => {
    const {spawnSync} = require('child_process');

    // console.log(req.body); { selectQuery: [ 3 ], queryNumber: '1' }
    let selectQuery = req.body.selectQuery;
    let queryNumber = req.body.queryNumber;

    let log_file_path = `nngp_log_join_${selectQuery}.txt`;
    let dot_file = `nngp_log_join_${selectQuery}.txt-q${queryNumber}.dot`;
    console.log("nngp: " + log_file_path);
    console.log("dot_file: " + dot_file);

    let eps = `/home/fypgf/gui/web/express/app/views/q${queryNumber}.eps`;

    // clearing previously generated pdf and relatived file
    const clear_pdf = await spawnSync('bash', [cfg["path"] + "/clear_pdf.sh"])
    console.log("clear_pdf stderr: " + clear_pdf.stderr.toString('ascii'));
    console.log("clear stdout: " + clear_pdf.stdout.toString('ascii'));
    console.log("clear exit code = ", clear_pdf.status);

    //  The following command will generate a dot file for each of the query.
    const generateDot =  spawnSync('bash', [cfg["path"] + "/generatePlanTree.sh", cfg["path"] + '/' +log_file_path, cfg["path"] + '/' + dot_file, eps, `/home/fypgf/gui/web/express/q${queryNumber}.pdf`]);
    console.log("generate dot stderr: " + generateDot.stderr.toString('ascii'));
   // console.log("generate stdout: " + generateDot.stdout.toString('ascii'));
    console.log("generate exit code = ", generateDot.status);


    res.send({ pdf: "http://" + cfg["host"] + ":" + cfg["port"] + "/" + cfg["img_relative_path"] + "/" + `q${queryNumber}.pdf` });
}




