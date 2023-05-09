const express = require("express");
const bodyParser = require("body-parser");
const cors = require("cors");

const path = __dirname + '/app/views/';

const app = express();

app.use(express.static(path));

var corsOptions = {
          origin: "http://localhost:8000"
};

app.use(cors(corsOptions));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const backend = require('./app/models/model.js');

app.get("/", (req, res) => {
 // res.json({ message: "Welcome to bezkoder application." });
 res.sendFile(path + "index.html");
  });

//  set port, listen for requests
require("./app/routes/routes.js")(app);
const PORT = process.env.PORT || 8000;
   app.listen(PORT, () => {
     console.log(`Server is running on port ${PORT}.`);
});
