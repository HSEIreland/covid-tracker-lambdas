const AWS = require('aws-sdk')
const SQL = require('@nearform/sql')
const { unflatten } = require('flat')
const { getAssetsBucket, getDatabase, runIfDev } = require('./utils')

async function getSettingsBody(client) {
  const sql = SQL`SELECT settings_key, settings_value FROM settings`
  const { rows } = await client.query(sql)
  const result = { generatedAt: new Date() }

  for (const { settings_key, settings_value } of rows) {
    result[settings_key] = settings_value
  }

  return unflatten(result)
}

exports.handler = async function () {
  const s3 = new AWS.S3({ region: process.env.AWS_REGION })
  const client = await getDatabase()
  const bucket = await getAssetsBucket()
  const settings = JSON.stringify(await getSettingsBody(client))

  const settingsObject = {
    ACL: 'private',
    Body: Buffer.from(settings),
    Bucket: bucket,
    ContentType: 'application/json',
    Key: 'settings.json'
  }

  await s3.putObject(settingsObject).promise()

  return settings
}

runIfDev(exports.handler)
