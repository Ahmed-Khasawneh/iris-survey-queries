const AWS = require('aws-sdk');
const ssh2 = require('ssh2');
const fs = require('fs-extra');
const Path = require('path');
const uuid = require('uuid');
const ora = require('ora');
const { argv } = require('yargs');
const moment = require('moment');
const SparkStageManager = require('./spark-stage-manager');

const OUTPUT_PATH_PARSER = /##OUTPUT##: (s3:\/\/[^\s]+)/
const S3_REGX_PARSER = /s3:\/\/([^\/]+)\/?(.*)/;

const SURVEY_FILE_MAP = {
  COMPLETIONS_1: ['completions', 'v1'],
  FALL_ENROLLMENT_1: ['fall', 'enrollment', 'v1'],
  FALL_ENROLLMENT_2: ['fall', 'enrollment', 'v2'],
  FALL_ENROLLMENT_3: ['fall', 'enrollment', 'v3'],
  FALL_ENROLLMENT_4: ['fall', 'enrollment', 'v4'],
  TWELVE_MONTH_ENROLLMENT_1: ['twelve', 'month', 'v1']
};

async function getS3Object({ uri, credentials }) {
  const { bucket, key } = parseS3Uri(uri);
  const s3 = new AWS.S3({ credentials });
  const response = await s3.getObject({
    Bucket: bucket,
    Key: key,
  }).promise();

  if (response.Body) {

    return response.Body.toString('utf8');
  }

  return '{}';
}

async function asPromise(method, ...args) {
  return new Promise((resolve, reject) => {
    method(...args, (err, ...args) => {
      if (err) {
        reject(err);
      } else if (args.length < 2) {
        resolve(args[0]);
      } else {
        resolve(args);
      }
    });
  });
}

function getSurveyTypeFromFileName(fileName) {
  return Object.keys(SURVEY_FILE_MAP).find(surveyType => {
    const words = SURVEY_FILE_MAP[surveyType];
    let foundAllWords = true;

    words.forEach(word => {
      if (!fileName.toLowerCase().includes(word.toLowerCase())) {
        foundAllWords = false;
      }
    });

    return foundAllWords;
  })
}

function parseS3Uri(s3Uri) {
  const groups = s3Uri.match(S3_REGX_PARSER);

  return {
    bucket: groups[1],
    key: groups[2],
  };
}

async function getFileContent(srcRelativePath) {
  return fs.readFile(Path.normalize('./' + srcRelativePath), 'utf8');
}

async function getPrivateKey({ credentials}) {
  const ssm = new AWS.SSM({ region: 'us-east-1', credentials });
  const result = await ssm.getParameter({
    Name: '/doris/shared/glue/dev-endpoint-private-key',
    WithDecryption: true,
  }).promise();

  return result.Parameter.Value;
}

async function getDevEndpointHost({ credentials }) {
  const glue = new AWS.Glue({ region: 'us-east-1', credentials });
  try {
    const result = await glue.getDevEndpoint({
      EndpointName: 'doris-test-endpoint-DEV',
    }).promise();
    
    return result.DevEndpoint.PublicAddress;
  } catch (e) {
    throw new Error('Glue Dev Endpoint not started. Run zeppelin automation to enable.');
  }
}

async function exec(conn, statement, options = {}) {
  return new Promise((resolve, reject) => {
    conn.exec(statement, function(err, stream) {
      if (err) {
        return reject(err);
      }
      stream
        .on('close', function(code, signal) {
          resolve();
        })
        .on('error', (e) => {
          reject(e);
        })
        .on('data', function(data) {
          if (options.onStdout) {
            options.onStdout(data);
          }
        })
        .stderr.on('data', function(data) {
          if (options.onStderr) {
            options.onStderr(data);
          }
        });
    });
  })
}

async function connect({ host, username, privateKey }) {
  const conn  = new ssh2.Client();
  return new Promise(async (resolve, reject) => {
    conn.on('ready', () => {
      resolve(conn);
    });
    conn.on('error', e => reject(e));
    conn.connect({
      host: host,
      username,
      privateKey: privateKey,
    });
  });
}

async function getCredentials() {
  const sts = new AWS.STS();
  const result = await sts.assumeRole({
    RoleArn: 'arn:aws:iam::102184641170:role/dev-iris',
    RoleSessionName: 'iris-dev-endpoint-testing'
  }).promise();
  const credentials = result.Credentials;

  return new AWS.Credentials(credentials.AccessKeyId, credentials.SecretAccessKey, credentials.SessionToken);
}

function getS3PathFromGlueOutput(output) {
  const groups = output.match(OUTPUT_PATH_PARSER);
  if (groups) {
    return groups[1].trim();
  }
}

async function putS3File({ body, uri, credentials }) {
  const { bucket, key } = parseS3Uri(uri);
  const s3 = new AWS.S3({ credentials });
  await s3.putObject({
    Bucket: bucket,
    Key: key,
    Body: body,
  }).promise();
}

async function putFile(conn, localPath, remotePath) {
  const sftp = await new Promise((resolve, reject) => conn.sftp((err, sftp) => err ? reject(err) : resolve(sftp)));
  await new Promise((resolve, reject) => sftp.fastPut(localPath, remotePath, (err) => err ? reject(err) : resolve()));
}

async function createSurveyFile({ reportUri, unitId, surveyType, stage = 'DEV', credentials }) {
  const lambda = new AWS.Lambda({ region: 'us-east-1', credentials });
  const response = await lambda.invoke({
    FunctionName: `iris-connector-doris-2019-${stage}-createSurveyFile`,
    Payload: JSON.stringify({
      surveyFileId: uuid.v4(),
      reportUri,
      unitId,
      surveyDefinitionType: surveyType,
    })
  }).promise();

  const body = JSON.parse(response.Payload || '{}');

  if (body.errorMessage) {
    let errorMessage = body.errorMessage;
    if (body.stackTrace) {
      errorMessage = body.stackTrace.join('\n');
    }
    throw new Error(errorMessage);
  } else {
    return body.surveyFile;
  }
}

async function main() {
  const spinner = ora().start('Preparing execution');
  let conn

  try {
    const surveyType = getSurveyTypeFromFileName(argv.sql);
    if (!surveyType) {
      throw new Error(`Unkonwn survey type for ${argv.sql}`);
    }
    const credentials = await getCredentials();
    // await writeKey(await getPrivateKey({}));
    conn = await connect({
      username: 'glue',
      host: await getDevEndpointHost({ credentials }),
      privateKey: await getPrivateKey({ credentials }),
    });
    // await asPromise(conn.forwardOut.bind(conn), '127.0.0.1', 4040, '127.0.0.1', 4040);
    await putFile(conn, Path.resolve(__dirname, '../pyspark/spark-executor.py'), '/home/glue/job.py');
  
    const sql = await getFileContent(argv.sql);
    const sqlUri = `s3://doris-survey-reports-dev/tmp-for-testing/sql-queries/${uuid.v4()}.sql`;

    await putS3File({ body: sql, uri: sqlUri, credentials });
    
    spinner.succeed();
    spinner.start('Executing SQL');
    
    const startTimeFormatted = moment().format('YYYY-MM-DD hh:mm:ss A');
    let outputString = '';
    const logFileName = `${startTimeFormatted}.txt`;
    const sparkStageManager = new SparkStageManager();
    await exec(conn, `/usr/bin/gluepython3 /home/glue/job.py --tenant_id=${argv.tenantId} --stage=${argv.stage} --sql=${sqlUri}`, {
      onStdout: async data => {
        const strData = data.toString();
        outputString += strData;
        if (argv.debug) {
          console.log(strData);
        }
        try {
          sparkStageManager.processChunk(data);
        } catch (e) {
          console.error(e.stack);
        }
        if (sparkStageManager.currentStage) {
          spinner.text = `Executing SQL - Stage ${sparkStageManager.currentStage}`;
        }
        await fs.ensureDir(Path.normalize('./.spark-logs'));
        await fs.appendFile(Path.normalize(`./.spark-logs/${logFileName}`), data, 'utf8');
      },
      onStderr: async data => {
        if (argv.debug) {
          console.error(data.toString())
        }
        await fs.ensureDir(Path.normalize('./.spark-logs'));
        await fs.appendFile(Path.normalize(`./.spark-logs/${logFileName}`), data);
      },
    });

    const s3OutputUri = getS3PathFromGlueOutput(outputString);

    if (!s3OutputUri) {
      throw new Error('Unable to get S3 output location');
    }

    const outputJson = JSON.parse(await getS3Object({ uri: s3OutputUri, credentials }));

    await fs.ensureDir(Path.normalize(`./.json-output/${surveyType}`));
    await fs.writeFile(
      Path.normalize(`./.json-output/${surveyType}/${startTimeFormatted}.json`),
      JSON.stringify(outputJson, null, 2),
      'utf8'
    );

    spinner.succeed(`Executing SQL - ${s3OutputUri}`);

    spinner.start('Validating Output');

    const surveyFile = await createSurveyFile({
      reportUri: s3OutputUri,
      unitId: 100654,
      surveyType,
      credentials,
      stage: argv.stage,
    });

    spinner.succeed();

    console.log(surveyFile);
  } catch (e) {
    spinner.fail(e.message);
  } finally {
    if (conn) {
      conn.end();
    }
  }

}

main().catch(() => null);