import express from "express";
import fs from "fs";
import readline from "readline";
import { connectDb } from "./utils/connectDb";
import equityModel from "./models/equity.model";
const app = express();

app.get("/", async (req, res) => {
  fs.readFile(
    "./data/20240129_Equity_Sample.txt",
    "utf-8",
    async (err, data) => {
      if (err) {
        console.log("err ------->", err);
        return res.status(400).json({
          success: false,
          message: "This is not going to help you !!",
        });
      }

      const dataLines = data.split("\r\n").map((line) => line.split("|"));

      const headers = dataLines[0];
      const finalData = [];

      for (let i = 1; i < dataLines.length; i++) {
        const line = dataLines[i];
        const obj = {};
        for (let i = 0; i < line.length; i++) {
          // @ts-ignore
          obj[headers[i]] = line[i];
        }
        finalData.push(obj);
      }

      const newData = await equityModel.insertMany(finalData);
      console.log("new Data", newData);

      res.status(200).json({
        success: true,
        message: "Data inserted Successfully ♪....♪...♫..♫.♫..♫..♪..♪...♪",
      });
    }
  );
});

app.get("/data", async (req, res) => {
  try {
    const data = await equityModel.find({}).countDocuments();

    res.status(200).json({
      success: true,
      message: "Data fechted successfully ♪..♫..♪..♫",
      data,
    });
  } catch (error) {
    console.log(error);
  }
});
app.get("/data/:id", async (req, res) => {
  const id = req.params.id;
  try {
    const data = await equityModel.findById(id);

    res.status(200).json({
      success: true,
      message: "Data fechted successfully ♪..♫..♪..♫",
      data,
    });
  } catch (error) {
    console.log(error);
  }
});
app.get("/property/:id", async (req, res) => {
  const id = req.params.id;
  try {
    const data = await equityModel
      .find({ PropertyID: id })
      .skip(40000)
      .limit(100);

    res.status(200).json({
      success: true,
      message: "Data fechted successfully ♪..♫..♪..♫",
      data,
    });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      success: false,
      message: "Something went wrong!!!",
      error,
    });
  }
});

app.get("/situsstreet/:name", async (req, res) => {
  const name = req.params.name;
  try {
    await equityModel.ensureIndexes();

    const data = await equityModel.find({ SitusStreet: name }).lean();

    res.status(200).json({
      success: true,
      message: "Data fechted successfully ♪..♫..♪..♫",
      data,
    });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      success: false,
      message: "Something went wrong!!!",
      error,
    });
  }
});

// ================ New Efficient Method ===========================
app.get("/eff-insert", async (req, res) => {
  try {
    const fileStream = fs.createReadStream("./data/20240129_Equity_Sample.txt");
    const rl = readline.createInterface({
      input: fileStream,
    });

    let batch = [];
    let headers = [];

    let batchCount = 0;
    for await (const line of rl) {
      if (headers.length === 0) {
        headers = line.split("|");
      } else {
        const lineArr = line.split("|");
        const obj = {};
        for (let i = 0; i < headers.length; i++) {
          // @ts-ignore
          obj[headers[i]] = lineArr[i];
        }

        batch.push(obj);

        // Insert batch into MongoDB in chunks of 1000 documents
        if (batch.length >= 100000) {
          await equityModel.insertMany(batch);
          batchCount++;
          batch = [];
          // if (batchCount === 2000) {
          //   return res.status(201).json({
          //     success: true,
          //     message: "1M  Data inserted Successfully ♪..♫.♫..♫..♪s",
          //   });
          // }
        }
      }
    }

    // Insert remaining documents
    if (batch.length > 0) {
      await equityModel.insertMany(batch);
    }

    res.status(200).json({
      success: true,
      message: `${
        batchCount * 1000 + batch.length
      } Data inserted Successfully ♫.♫..♫..♪`,
    });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      success: false,
      message: "Data not inserted !!!",
      error,
    });
  }
});

// =============== find by name ============================
app.get("/find-data", async (req, res) => {
  try {
    const fileStream = fs.createReadStream("./data/20240129_Equity_Sample.txt");
    const rl = readline.createInterface({
      input: fileStream,
    });

    let headers = [];

    for await (const line of rl) {
      if (headers.length === 0) {
        headers = line.split("|");
      } else {
        const lineArr = line.split("|");
        const obj: any = {};
        for (let i = 0; i < headers.length; i++) {
          // @ts-ignore
          obj[headers[i]] = lineArr[i];
        }
        if (obj["PropertyID"] === "19084352") {
          return res.status(200).json({
            success: true,
            message: ` Data found Successfully ♫.♫..♫..♪`,
            data: obj,
          });
        }
      }
    }

    res.status(200).json({
      success: true,
      message: ` Data not found  ♫.♫..♫..♪`,
      data: null,
    });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      success: false,
      message: "Data not found !!!",
      error,
    });
  }
});

// =============== find by property eff ============================
app.get("/find-data-eff", async (req, res) => {
  const PropertyID = req.query.PropertyID;
  try {
    const fileStream = fs.createReadStream("./data/20240129_Equity_Sample.txt");
    const rl = readline.createInterface({
      input: fileStream,
    });

    let headers = [];
    const data: any[] = [];

    for await (const line of rl) {
      if (headers.length === 0) {
        headers = line.split("|");
      } else {
        const lineArr = line.split("|");

        if (lineArr[1] === PropertyID) {
          const obj: any = {};
          for (let i = 0; i < headers.length; i++) {
            // @ts-ignore
            obj[headers[i]] = lineArr[i];
          }
          data.push(obj);
          return res.status(200).json({
            success: true,
            message: ` Data found Successfully ♫.♫..♫..♪`,
            data,
          });
        }
      }
    }
    return res.status(200).json({
      success: true,
      message: ` Data found Successfully ♫.♫..♫..♪`,
      data,
    });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      success: false,
      message: "Data not found !!!",
      error,
    });
  }
});

app.get("/find-data-with-db", async (req, res) => {
  try {
    const data = await equityModel.find({ PropertyID: "19084352" });

    return res.status(200).json({
      success: true,
      message: ` Data found Successfully ♫.♫..♫..♪`,
      data,
    });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      success: false,
      message: "Data not found !!!",
      error,
    });
  }
});

// =============== Text to json ============================
app.get("/text-to-json", async (req, res) => {
  try {
    const fileStream = fs.createReadStream("./data/20240129_Equity_Sample.txt");
    const rl = readline.createInterface({
      input: fileStream,
    });
    const writeStream = fs.createWriteStream("./parseData/equity_sample.json");

    // const jsonArray = [];
    let headers = [];

    for await (const line of rl) {
      if (headers.length === 0) {
        headers = line.split("|");
      } else {
        const lineArr = line.split("|");
        const obj: any = {};
        for (let i = 0; i < headers.length; i++) {
          // @ts-ignore
          obj[headers[i]] = lineArr[i];
        }
        // jsonArray.push(obj);
        writeStream.write(JSON.stringify(obj) + "\n", (err) => {
          if (err) {
            console.error("Error writing to the file:", err);
          }
        });
      }
    }

    writeStream.end();
    // writeStream.write(JSON.stringify(jsonArray), (err) => {
    //   if (err) {
    //     console.error("Error writing to the file:", err);
    //   } else {
    //     console.log("JSON file has been created successfully.");
    //   }
    // });

    res.status(200).json({
      success: true,
      message: `Data parsed and write successfully  ♫.♫..♫..♪`,
    });
  } catch (error) {
    console.log(error);
    res.status(500).json({
      success: false,
      message: "Data could not parsed !!!",
      error,
    });
  }
});

app.get("/get-json", (req, res) => {
  fs.readFile("./parseData/equity_sample.json", "utf-8", (err, data) => {
    if (err) {
      console.log(err);
      res.status(500).json({
        success: false,
        message: "Data could not read !!!",
        err,
      });
    }
    res.status(200).json({
      success: true,
      message: `Data read  successfully  ♫.♫..♫..♪`,
      data: data,
    });
  });
});

// ===========================================

app.listen(3001, () => {
  console.log(`server is running at port : 3001`);
  connectDb("mongodb://127.0.0.1:27017/proptechProData");
});
