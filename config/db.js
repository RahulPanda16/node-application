const { MongoClient, ServerApiVersion } = require("mongodb");

const url = process.env.MONGO_URL 
// || "mongodb+srv://goofy:neutrenz123@cluster0.wemic.mongodb.net/";

const client = new MongoClient(url, {
  serverApi: {
    version: ServerApiVersion.v1,
    strict: true,
    deprecationErrors: true,
  },
});

const databaseName = "node-crud";

const connection = async () => {
  try {
    await client.connect();
    console.log("Connected successfully to MongoDB server");
    const db = client.db(databaseName);
    return db;
  } catch (error) {
    console.error("MongoDB connection error:", error);
  }
};

module.exports = connection;
