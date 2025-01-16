const { Client } = require("@elastic/elasticsearch");
const { faker } = require("@faker-js/faker");
const { createReport } = require("../utils/createReport");

const client = new Client({
  node: process.env.ELASTIC_SEARCH,
});

const createElasticData = async (req, res) => {
  const { firstname, lastname, email, password } = req.body;

  try {
    const response = await client.index({
      index: "userrahul",
      body: { firstname, lastname, email, password },
    });
    res.json({
      message: "Successfully Added",
      data: response,
    });
  } catch (error) {
    res.json({
      message: "Failed to save",
      error: error,
    });
  }
};

const createIndex = async (req, res) => {
  const index = req.body.username;
  await client.indices.create({ index: index });

  res.json({
    message: "Successfully created index",
  });
};

const readElasticDataAll = async (req, res) => {
  try {
    const response = await client.search({ index: "rahul" });

    res.json({
      message: "Success",
      data: response,
    });
  } catch (error) {
    res.json({
      message: "Failed",
      error: error,
    });
  }
};

const readElasticDataByIndex = async (req, res) => {
  const index = req.params.index;

  try {
    const response = await client.search({ index: index });

    res.json({
      message: "Success",
      data: response,
    });
  } catch (error) {
    res.json({
      message: "Failed",
      error: error,
    });
  }
};

const updateElasticData = async (req, res) => {
  const { firstname, lastname, email } = req.body;
  const index = req.params.index;
  const id = req.params.id;

  const data = { firstname, lastname, email };

  const response = await client.update({
    index: index,
    id: id,
    doc: data,
  });

  res.json({
    message: "Successfully Updated",
    data: response,
  });
};

const deleteElasticData = async (req, res) => {
  const index = req.params.index;
  const id = req.params.id;

  await client.delete({
    index: index,
    id: id,
  });

  res.json({
    message: "Successfully Deleted",
  });
};

const deleteElasticIndex = async (req, res) => {
  const index = req.params.index;
  await client.indices.delete({ index: index });

  res.json({
    message: "Successfully deleted",
  });
};

const createElasticBulkData = async (req, res) => {
  let bulkData = [];
  for (let i = 0; i < 100000; i++) {
    const firstname = faker.internet.username();
    const lastname = faker.internet.username();
    const email = faker.internet.email();
    // const password = faker.internet.password();
    const gender = faker.person.sex();
    const zodiacSign = faker.person.zodiacSign();
    const epocTime = new Date().valueOf();
    const date = new Date().toLocaleDateString();
    const time = new Date().toLocaleTimeString();

    bulkData.push({ index: { _index: "rahul-detail" } });
    bulkData.push({
      firstname,
      lastname,
      email,
      gender,
      zodiacSign,
      epocTime,
      date,
      time,
    });
  }

  try {
    const response = await client.bulk({ body: bulkData });
    if (response.errors) {
      res.json({
        message: "Failed to save some or all documents",
        error: response.errors,
      });
    } else {
      res.json({
        message: "Successfully Added",
        data: response,
      });
    }
  } catch (error) {
    res.json({ message: "Failed to save", error: error });
  }
};

const getElasticBulkData = async (req, res) => {
  const gender = req.params.gender;
  const zsign = req.params.zsign;
  const response = await client.search({
    index: "rahul-detail",
    body: {
      query: {
        bool: {
          must: [{ term: { gender: gender } }, { term: { zsign: zsign } }],
        },
      },
    },
  });

  res.json({ message: "success", response });
};

const createElasticReport = async (req, res) => {
  const reportData = createReport();
  const bulkData = [];

  reportData.forEach((doc) => {
    bulkData.push({ index: { _index: "rahul-reportdata" } });
    bulkData.push(doc);
  });

  try {
    const response = await client.bulk({ body: bulkData });
    if (response.errors) {
      const erroredDocuments = response.items.filter(
        (item) => item.index && item.index.error
      );

      res.json({
        message: "Failed to save some or all documents",
        error: erroredDocuments,
      });
    } else {
      res.json({
        message: "Successfully Added",
        data: response,
      });
    }
  } catch (error) {
    res.json({ message: "Failed to save", error: error.message });
  }
};

const getElasticAllReport = async (req, res) => {
  try {
    const response = await client.search({
      index: "rahul-reportdata",
      body: {
        size: 10000,
        query: {
          match_all: {},
        },
      },
    });

    res.send(response.hits.hits.map((hit) => hit._source));
  } catch (error) {
    console.error(error);
    res.json({ message: "Error", data: error });
  }
};

module.exports = {
  client,
  createIndex,
  createElasticData,
  readElasticDataAll,
  readElasticDataByIndex,
  updateElasticData,
  deleteElasticData,
  deleteElasticIndex,
  createElasticBulkData,
  getElasticBulkData,
  createElasticReport,
  getElasticAllReport,
};
