const AWS = require('aws-sdk')
const fetch = require('node-fetch')
const querystring = require('querystring')
const { getCallbackConfig, runIfDev } = require('./utils')

exports.handler = async function (event) {
  const sqs = new AWS.SQS({ region: process.env.AWS_REGION })
  const { accessGuid, apiVersion, sig, sp, sv, queueUrl, url } = await getCallbackConfig()

  for (const record of event.Records) {
    const { closeContactDate, failedAttempts, id, mobile, payload } = JSON.parse(record.body)

    try {
      const query = querystring.stringify({
        'api-version': apiVersion,
        'sp': sp,
        'sv': sv,
        'sig': sig
      })

      const body = JSON.stringify({
        'PhoneMobile': mobile,
        'DateLastContact': new Date(closeContactDate + 43200000).toISOString().substr(0, 10),
        'Payload': payload
      })

      await fetch(`${url}/${accessGuid}?${query}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body
      })
    } catch (error) {
      if (failedAttempts <= 3) {
        const message = {
          QueueUrl: queueUrl,
          MessageBody: JSON.stringify({
            closeContactDate,
            failedAttempts: failedAttempts + 1,
            id,
            mobile,
            payload
          }),
          DelaySeconds: (failedAttempts + 1) * 60
        }

        await sqs.sendMessage(message).promise()
      }
    }
  }

  return true
}

runIfDev(exports.handler)
