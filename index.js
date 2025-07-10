const fs = require("fs");
const { parse } = require("json2csv");
const zlib = require("zlib");
const readline = require("node:readline");
const { unmarshall } = require("@aws-sdk/util-dynamodb");

const OUTPUT_FILENAME = "output_combined.csv";
const FIELDS = ["PK", "SK", "country", "userId"];

const DATA_TYPE = {
  NOTIFICATION: "notification",
  PROFILE: "profile",
  USER_SEGMENT: "userSegment",
};

function handleNotification(data) {
  if (data.language != "vi") return null;
  return {
    PK: `COUNTRY#CZ`,
    SK: `#USER#${data.userId}`,
    country: `CZ`,
    userId: data.userId,
  };
}

function handleUserSegment(data) {
  return {
    PK: data.PK,
    SK: data.SK,
    country: data.country,
    userId: data.userId,
  };
}

function handleUserProfile(data) {
  if (data.country != "VN") return null;
  return {
    PK: `COUNTRY#${data.country}`,
    SK: `#USER#${data.userId}`,
    country: data.country,
    userId: data.userId,
  };
}

const handlers = {
  [DATA_TYPE.NOTIFICATION]: handleNotification,
  [DATA_TYPE.PROFILE]: handleUserProfile,
  [DATA_TYPE.USER_SEGMENT]: handleUserSegment,
};

function processDynamoDBData(line, transformFlag) {
  if (!line) return;
  const parsed = JSON.parse(line);
  const data = unmarshall(parsed.Item);
  return handlers[transformFlag]?.(data);
}

const statistics = {};

async function processDynamoDBJsonFiles(
  fileList,
  OUTPUT_FILENAME,
  transformFlag
) {
  for (const file of fileList) {
    console.log(`Processing file: \x1b[36m${file}\x1b[0m`);
    if (!fs.existsSync(file)) {
      console.error(`\x1b[31mFile not found: ${file}\x1b[0m`);
      continue;
    }

    const readStream = fs.createReadStream(file);
    const unzipStream = readStream.pipe(zlib.createGunzip());

    const rl = readline.createInterface({
      input: unzipStream,
      crlfDelay: Infinity,
    });

    const outputStream = fs.createWriteStream(OUTPUT_FILENAME, { flags: "a" });

    let lineCount = 0;
    rl.on("line", (line) => {
      lineCount++;
      process.stdout.write(
        `\r\x1b[32m${file}\x1b[0m - line \x1b[33m${lineCount}\x1b[0m`
      );
      const data = processDynamoDBData(line, transformFlag);
      if (!data) return;
      statistics[data.country] = (statistics[data.country] || 0) + 1;
      const csv = parse([data], {
        fields: ["PK", "SK", "country", "userId"],
        header: false,
      });
      outputStream.write(csv + "\n");
    });

    await new Promise((resolve) => {
      rl.on("close", () => {
        process.stdout.write("\n");
        outputStream.end();
        resolve();
      });
    });
  }
}

async function start() {
  if (fs.existsSync(OUTPUT_FILENAME)) {
    console.log(
      `\x1b[31m${OUTPUT_FILENAME}\x1b[0m does exist. Delete it before start.`
    );
    return;
  }

  fs.writeFileSync(
    OUTPUT_FILENAME,
    parse([], { fields: FIELDS, header: true }) + "\n"
  );

  console.log("Processing Notification .json.gz files...");
  const userNotification = [
    "./data/notifications/3gwsd3mpzm4g3o577pkqajynzm.json.gz",
    "./data/notifications/3mplb62geq4hrkxdygbpgws3ra.json.gz",
    "./data/notifications/6dph5ocfbmzxpbcj3oah6hvrxu.json.gz",
    "./data/notifications/y3gmlzyqvq5xtpmoeei3jhraka.json.gz",
  ];
  await processDynamoDBJsonFiles(
    userNotification,
    OUTPUT_FILENAME,
    DATA_TYPE.NOTIFICATION
  );

  console.log("Processing UserSegment .json.gz files...");
  const userSegments = [
    "./data/user_segments/p5hvavbzdu2iphoqyezmh53fri.json.gz",
  ];
  await processDynamoDBJsonFiles(
    userSegments,
    OUTPUT_FILENAME,
    DATA_TYPE.USER_SEGMENT
  );

  console.log("Processing UserProfile .json.gz files...");
  const userProfiles = [
    "./data/user_profiles/3ubo7oa3dq26tdriq34polqili.json.gz",
    "./data/user_profiles/cfpo6voetayxpljgrctpuv4ivi.json.gz",
    "./data/user_profiles/l76iacmd6a5ujdmm2d4dtqiypa.json.gz",
    "./data/user_profiles/t7x6tatmre6wllbubtdhrqup6q.json.gz",
  ];
  await processDynamoDBJsonFiles(
    userProfiles,
    OUTPUT_FILENAME,
    DATA_TYPE.PROFILE
  );

  console.log(
    "\x1b[42m\x1b[30mFINISH:\x1b[0m Data processed and combined into a single CSV file"
  );

  console.log(`Statistics by country:`);
  Object.keys(statistics).forEach((country) => {
    console.log(
      `\x1b[32mCountry: ${country}\x1b[0m, Count: \x1b[33m${statistics[country]}\x1b[0m`
    );
  });

  const gzippedData = await gzip(fs.readFileSync(OUTPUT_FILENAME));
  fs.writeFileSync(OUTPUT_FILENAME + ".gzip", gzippedData);

  console.log(
    `Output written to ${OUTPUT_FILENAME} and gzipped to ${OUTPUT_FILENAME}.gz`
  );
}

start().catch((error) => {
  console.error("Error processing data:", error);
});
