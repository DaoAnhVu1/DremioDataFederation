const { Sequelize } = require("sequelize");
const fs = require("fs");
const csv = require("csv-parser");

const connectionConfig = {
  host: "mypostgres_database",
  user: "postgres",
  password: "password",
  database: "top_movies",
};

const sequelize = new Sequelize(
  connectionConfig.database,
  connectionConfig.user,
  connectionConfig.password,
  {
    host: connectionConfig.host,
    dialect: "postgres",
  }
);

async function connectWithRetries() {
  for (let index = 0; index < 5; index++) {
    try {
      await sequelize.authenticate();
      console.log("Connection has been established successfully.");
      return;
    } catch (error) {
      console.error("Failed to connect to the database:", error);
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  }
  console.error("Maximum number of connection retries reached. Exiting...");
}

async function createTables() {
  const createMoviesTableQuery = `
        CREATE TABLE IF NOT EXISTS top_movies (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            release_year INT,
            runtime INT,
            imdb_rating FLOAT,
            director VARCHAR(255),
            genre VARCHAR(100),
            overview TEXT,
            metascore INT,
            number_of_votes INT,
            gross BIGINT
        );
    `;

  try {
    await sequelize.query(createMoviesTableQuery);
    console.log("Table has been created.");
  } catch (error) {
    console.error("Error creating table:", error);
  }
}

async function getRecordCount() {
  const countQuery = `
        SELECT COUNT(*) AS count FROM top_movies;
    `;
  const [results] = await sequelize.query(countQuery);
  return results[0].count;
}

async function main() {
  try {
    // Wait 5 seconds before trying to connect
    await new Promise((resolve, reject) => setTimeout(resolve, 5000));
    await connectWithRetries();

    await createTables();

    const recordCount = await getRecordCount();
    if (recordCount > 10000) {
      console.log(
        "There are already more than 10000 records in the database. Data load aborted."
      );
      return;
    }

    const dirPath = "./data/top_1000_imdb/imdb_top_1000.csv";
    const stream = fs.createReadStream(dirPath).pipe(csv());

    for await (const data of stream) {
      let name = data["Series_Title"];
      let year = parseInt(data["Released_Year"]);
      let runtime = parseInt(data["Runtime"].split(" ")[0]);
      let genre = data["Genre"];
      let imdb_rating = parseFloat(data["IMDB_Rating"]);
      let overview = data["Overview"];
      let metascore = parseInt(data["Meta_score"]);
      let director = data["Director"];
      let noVotes = parseInt(data["No_of_Votes"]);
      let gross = parseInt(data["Gross"].replace(/,/g, ""));

      try {
        await sequelize.query(
          `
                INSERT INTO top_movies (name, release_year, runtime, genre, imdb_rating, overview, metascore, director, number_of_votes, gross)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              `,
          {
            replacements: [
              name,
              year,
              runtime,
              genre,
              imdb_rating,
              overview,
              metascore,
              director,
              noVotes,
              gross,
            ],
          }
        );

        console.log(`Record inserted: ${name}`);
      } catch (error) {
        console.log("ERROR INSERTING: " + error);
      }
    }

    console.log("FINISHED");
  } catch (error) {
    console.log("Error occurred:", error);
  }
}

main();
