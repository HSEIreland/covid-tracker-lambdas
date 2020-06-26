const jwt = require('jsonwebtoken')
const { getJwtSecret, runIfDev } = require('./utils')

function isAuthorized(token, secret) {
  try {
    const data = jwt.verify(token.replace(/^Bearer /, ''), secret)

    if (data.refresh || !data.id) {
      return false
    }

    return true
  } catch (error) {
    return false
  }
}

exports.handler = async function (event) {
  const secret = await getJwtSecret()

  if (!isAuthorized(event.authorizationToken, secret)) {
    throw 'Unauthorized'
  }

  return {
    principalId: event.authorizationToken,
    policyDocument: {
      Version: '2012-10-17',
      Statement: [
        {
          Action: 'execute-api:Invoke',
          Effect: 'Allow',
          Resource: 'arn:aws:execute-api:*'
        }
      ]
    }
  }
}

runIfDev(exports.handler)
