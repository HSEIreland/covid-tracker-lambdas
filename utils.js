const AWS = require('aws-sdk')
const pg = require('pg')

const isProduction = /^\s*production\s*$/i.test(process.env.NODE_ENV)
const ssm = new AWS.SSM({ region: process.env.AWS_REGION })
const secretsManager = new AWS.SecretsManager({ region: process.env.AWS_REGION })

async function getParameter(id) {
  const response = await ssm
    .getParameter({ Name: `${process.env.CONFIG_VAR_PREFIX}${id}` })
    .promise()

  return response.Parameter.Value
}

async function getSecret(id) {
  const response = await secretsManager
    .getSecretValue({ SecretId: `${process.env.CONFIG_VAR_PREFIX}${id}` })
    .promise()

  return JSON.parse(response.SecretString)
}

async function getAssetsBucket() {
  if (isProduction) {
    return await getParameter('s3_assets_bucket')
  } else {
    return process.env.ASSETS_BUCKET
  }
}

async function getCallbackConfig() {
  if (isProduction) {
    return {
      ...await getSecret('cct'),
      queueUrl: await getParameter('callback_url')
    }
  } else {
    return {
      url: process.env.CCT_URL,
      accessGuid: process.env.CCT_ACCESS_GUID,
      apiVersion: process.env.CCT_API_VERSION,
      sp: process.env.CCT_SP,
      sv: process.env.CCT_SV,
      sig: process.env.CCT_SIG,
      queueUrl: process.env.CALLBACK_QUEUE_URL
    }
  }
}

async function getCsoConfig() {
  if (isProduction) {
    return await getSecret('cso')
  } else {
    return {
      publicKey: process.env.CSO_PUBLIC_KEY,
      host: process.env.CSO_SFTP_HOST,
      port: process.env.CSO_SFTP_PORT,
      username: process.env.CSO_SFTP_USER,
      password: process.env.CSO_SFTP_PASSWORD,
      checkInPath: process.env.CSO_CHECK_IN_PATH
    }
  }
}

async function getDatabase() {
  require('pg-range').install(pg)

  let client

  if (isProduction) {
    const [{ username: user, password }, host, port, ssl, database] = await Promise.all([
      getSecret('rds'),
      getParameter('db_host'),
      getParameter('db_port'),
      getParameter('db_ssl'),
      getParameter('db_database')
    ])

    client = new pg.Client({
      host,
      database,
      user,
      password,
      port: Number(port),
      ssl: ssl === 'true'
    })
  } else {
    const { user, password, host, port, ssl, database } = {
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      host: process.env.DB_HOST,
      port: Number(process.env.DB_PORT),
      ssl:  /true/i.test(process.env.DB_SSL),
      database: process.env.DB_DATABASE
    }

    client = new pg.Client({
      host,
      database,
      user,
      password,
      port: Number(port),
      ssl: ssl === 'true'
    })
  }

  await client.connect()

  return client
}

async function getEncryptKey() {
  if (isProduction) {
    const { key } = await getSecret('encrypt')

    return key
  } else {
    return process.env.ENCRYPT_KEY
  }
}

async function getExposuresConfig() {
  if (isProduction) {
    const [
      { privateKey, signatureAlgorithm, verificationKeyId, verificationKeyVersion },
      appBundleId
    ] = await Promise.all([
      getSecret('exposures'),
      getParameter('app_bundle_id')
    ])

    return { appBundleId, privateKey, signatureAlgorithm, verificationKeyId, verificationKeyVersion }
  } else {
    return {
      appBundleId: process.env.APP_BUNDLE_ID,
      privateKey: process.env.EXPOSURES_PRIVATE_KEY,
      signatureAlgorithm: process.env.EXPOSURES_SIGNATURE_ALGORITHM,
      verificationKeyId: process.env.EXPOSURES_KEY_ID,
      verificationKeyVersion: process.env.EXPOSURES_KEY_VERSION
    }
  }
}

async function getJwtSecret() {
  if (isProduction) {
    const { key } = await getSecret('jwt')

    return key
  } else {
    return process.env.JWT_SECRET
  }
}

async function getStatsUrl() {
  if (isProduction) {
    return await getParameter('arcgis_url')
  } else {
    return process.env.STATS_URL
  }
}

function runIfDev(fn) {
  if (!isProduction) {
    fn(JSON.parse(process.argv[2] || '{}'))
      .then(result => {
        console.log(result)
        process.exit(0)
      })
      .catch(error => {
        console.log(error)
        process.exit(1)
      })
  }
}

module.exports = {
  getAssetsBucket,
  getCallbackConfig,
  getCsoConfig,
  getDatabase,
  getEncryptKey,
  getExposuresConfig,
  getJwtSecret,
  getStatsUrl,
  runIfDev
}
