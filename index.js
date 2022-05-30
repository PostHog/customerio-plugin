const eventsConfig = {
    SEND_ALL: '1',
    SEND_EMAILS: '2',
    SEND_IDENTIFIED: '3'
}

const EVENTS_CONFIG_MAP = {
    'Send all events': eventsConfig.SEND_ALL,
    'Only send events from users with emails': eventsConfig.SEND_EMAILS,
    'Only send events from users that have been identified': eventsConfig.SEND_IDENTIFIED
}

class RetryError extends Error {}

export async function setupPlugin({ config, global }) {
    const customerioBase64AuthToken = Buffer.from(`${config.customerioSiteId}:${config.customerioToken}`).toString(
        'base64'
    )

    global.customerioAuthHeader = {
        headers: {
            Authorization: `Basic ${customerioBase64AuthToken}`
        }
    }

    const authResponse = await fetchWithRetry(
        'https://beta-api.customer.io/v1/api/info/ip_addresses',
        global.customerioAuthHeader
    )

    if (!authResponse || statusUnauthorized(authResponse)) {
        let response = authResponse
        if (response) {
            response = await authResponse.json()
        }
        console.error(`Unable to connect to Customer.io - Response = ${response}`)
        return
    }

    if (!statusOk(authResponse)) {
        let response = await authResponse.json()
        console.error(`Service is down, retry later - Response = ${response}`)
        return
    }

    global.eventNames = config.eventsToSend ? config.eventsToSend.split(',').filter(Boolean) : []

    global.eventsConfig = EVENTS_CONFIG_MAP[config.sendEventsFromAnonymousUsers]
}

export async function exportEvents(events, meta) {
    const { global, config } = meta

    // KLUDGE: This shouldn't even run if setupPlugin failed. Needs to be fixed at the plugin server level
    if (!global.eventNames) {
        throw new RetryError('setupPlugin failed. Cannot run exportEvents.')
    }

    console.log(`Flushing batch of length ${events.length}`)
    for (const event of events) {
        if (
            (global.eventNames.length > 0 && !global.eventNames.includes(event.event)) ||
            (global.eventsConfig === eventsConfig.SEND_IDENTIFIED && isAnonymousUser(event))
        ) {
            continue
        }

        try {
            await exportToCustomerio(event, global.customerioAuthHeader, config.host, meta)
        } catch (error) {
            console.error('Failed to export to Customer.io')
        }
    }
}

async function exportToCustomerio(payload, authHeader, customerioHost, { cache, global }) {
    const { event, distinct_id } = payload

    const properties = { ...payload.properties, ...(payload.properties.$set || {}) }

    const baseCustomersURL = `https://${customerioHost}/api/v1/customers/${distinct_id}`
    const headers = { 'Content-Type': 'application/x-www-form-urlencoded', ...authHeader.headers }

    let userExists = await cache.get(distinct_id, false)
    const isIdentifyEvent = event === '$identify'
    const email = isIdentifyEvent && getEmailFromEvent(payload)

    const shouldCreateUserForConfig =
        global.eventsConfig === eventsConfig.SEND_ALL ||
        (global.eventsConfig === eventsConfig.SEND_EMAILS && email) ||
        (global.eventsConfig === eventsConfig.SEND_IDENTIFIED && isIdentifyEvent)

    if (!userExists && shouldCreateUserForConfig) {
        userExists = await createCustomerioUserIfNotExists(baseCustomersURL, email, properties, headers)
        if (userExists) {
            await cache.set(distinct_id, true, 60 * 5) // 5 minutes
        }
    }

    if (userExists) {
        await sendEventToCustomerIo(`${baseCustomersURL}/events`, event, properties, headers)
    }
}

async function sendEventToCustomerIo(url, event, properties, headers) {
    const body = JSON.stringify({ name: event, data: properties })
    const response = await fetchWithRetry(`${url}/events`, { headers, body }, 'POST')
    if (!statusOk(response)) {
        console.error(`Unable to send event ${event} to Customer.io`)
    }
}

// Customer.io will just update the user and return `ok` if it already exists
async function createCustomerioUserIfNotExists(url, email, properties, headers) {
    const body = JSON.stringify({ email, ...properties })
    try {
        const response = await fetchWithRetry(url, { headers, body }, 'PUT')
        if (!statusOk(response)) {
            console.error(`Unable to create user with email ${email}. Status: ${response.status}.`)
            return false
        }
        return true
    } catch (error) {
        console.error(error)
    }

    return false
}

async function fetchWithRetry(url, options = {}, method = 'GET', isRetry = false) {
    try {
        const res = await fetch(url, { method: method, ...options })
        return res
    } catch {
        if (isRetry) {
            console.error(`${method} request to ${url} failed.`)
            return
        }
        const res = await fetchWithRetry(url, options, (method = method), (isRetry = true))
        return res
    }
}

function statusOk(res) {
    return String(res.status)[0] === '2'
}

function statusUnauthorized(res) {
    return res.status == 401
}

function isEmail(email) {
    const re = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
    return re.test(String(email).toLowerCase())
}

function isAnonymousUser({ distinct_id, properties }) {
    if (properties) return properties['$device_id'] === distinct_id

    // A fallback in case the event doesn't have `properties` set, for some reason.
    const re = /^[\w]{14}-[\w]{14}-[\w]{8}-[\w]{6}-[\w]{14}$/g
    return re.test(String(distinct_id))
}

function getEmailFromEvent(event) {
    if (isEmail(event.distinct_id)) {
        return event.distinct_id
    }

    const getEmailFromKey = (key) => {
        const object = event[key]
        if (!object) return false
        if (!Object.keys(object).includes('email')) return false

        const email = object['email']
        return isEmail(email) && email
    }

    return getEmailFromKey('$set') || getEmailFromKey('$set_once') || getEmailFromKey('properties')
}
