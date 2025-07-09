const fs = require("fs");
const { load } = require("ion-js");
const { parse } = require("json2csv");
const { ungzip, gzip } = require("node-gzip");

function processIonData(data, transformFlag) {
  const items = [];
  let currentItem = {};
  data.forEach((item) => {
    if (item.Item) {
      if (Object.keys(currentItem).length !== 0) {
        items.push(currentItem);
        currentItem = {};
      }
      currentItem = item.Item;
    }
  });

  if (transformFlag) {
    return items
      .map((item) => ({
        PK: `COUNTRY#${item.country}`,
        SK: `#USER#${item.userId}`,
        country: item.country,
        userId: item.userId,
      }))
      .filter((item) => item.country == "VN"); // filter for country
  } else {
    return items.map((item) => ({
      PK: item.PK,
      SK: item.SK,
      country: item.country,
      userId: item.userId,
    }));
  }
}

async function processIonGzFiles(fileList, outputFileName, transformFlag) {
  const combinedData = [];

  for (const file of fileList) {
    console.log(`Processing file: ${file}`);
    if (!fs.existsSync(file)) {
      console.error(`File not found: ${file}`);
      continue;
    }
    const data = await ungzip(fs.readFileSync(file, null));
    const decodedData = data.toString("utf8");
    const lines = decodedData.split("\n").filter((line) => line.trim() !== "");

    const ionData = lines.map(load);

    const processData = processIonData(ionData, transformFlag);
    if (processData.length > 0) {
      combinedData.push(...processIonData(ionData, transformFlag));
    }
  }

  const isOutputFileExist = fs.existsSync(outputFileName);

  const csv = parse(combinedData, {
    fields: ["PK", "SK", "country", "userId"],
    header: !isOutputFileExist,
  });

  fs.appendFileSync(outputFileName, isOutputFileExist ? "\n" + csv : csv);
}

async function start() {
  const outputFileName = "output_combined.csv";
  console.log("Processing UserSegments .ion.gz files...");
  const userSegments = [
    "./data/user_segments/44dazuseku5e5pc4chdr22iqy4.ion.gz",
  ];
  await processIonGzFiles(userSegments, outputFileName, false);

  console.log("Processing UserProfiles .ion.gz files...");
  const userProfiles = [
    "./data/user_profiles/i6cqcxv7y455njoqgzpws47sf4.ion.gz",
    "./data/user_profiles/ooalwe6jly7hvmbdkaaqkopmbu.ion.gz",
    "./data/user_profiles/pmhqknwzxa2ljnypmtbckxsr5e.ion.gz",
    "./data/user_profiles/zv4bixxjry2hppgopemle7saoy.ion.gz",
  ];
  await processIonGzFiles(userProfiles, outputFileName, true);

  console.log("Data processed and combined into a single CSV file");

  const gzippedData = await gzip(fs.readFileSync(outputFileName));
  fs.writeFileSync(outputFileName + ".gzip", gzippedData);

  console.log(
    `Output written to ${outputFileName} and gzipped to ${outputFileName}.gz`
  );
}

start().catch((error) => {
  console.error("Error processing data:", error);
});
