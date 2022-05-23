import { createBuffer } from '@posthog/plugin-contrib'

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
    
    if (statusUnauthorized(authResponse)){
        throw new Error('Unable to connect to Customer.io')
    }

    if (!statusOk(authResponse)) {
        throw new RetryError('Service is down, retry later')
    }

    const eventNames = config.eventsToSend ? config.eventsToSend.split(',').filter(Boolean) : []

    global.buffer = createBuffer({
        limit: (1 / 5) * 1024 * 1024, // 200kb
        timeoutSeconds: 5,
        onFlush: async (batch) => {
            console.log(`Flushing batch of length ${batch.length}`)
            for (const event of batch) {
                if (eventNames.length > 0 && !eventNames.includes(event.event)) { 
                    continue
                }

                try {
                    await exportToCustomerio(event, global.customerioAuthHeader)
                } catch (error) {
                    console.error("Failed to export to Customer.io with error", error.message)
                }
            }
        }
    })
}

export async function onEvent(event, { config, global }) {
    if (
        config.sendEventsFromAnonymousUsers === "Only send events from users that have been identified" &&
        isAnonymousUser(event)
    ) {
        return
    }

    console.log('onEvent', event.event, event.distinct_id)
    global.buffer.add(event)
}

async function exportToCustomerio(payload, authHeader) {
    const { event, distinct_id, properties } = payload

    const baseCustomersURL = `https://track.customer.io/api/v1/customers/${distinct_id}`
    const headers = { 'Content-Type': 'application/x-www-form-urlencoded', ...authHeader.headers }

    const isIdentifyEvent = event === '$identify'
    const email = isIdentifyEvent && getEmailFromEvent(payload)
    

    let response

    if (isIdentifyEvent) {
        if (!email) {
            console.log(`Email not found in user ${distinct_id}. Unable to create customer in Customer.io`)
            return
        }

        const body = JSON.stringify({ email, ...properties })
        response = await fetchWithRetry(baseCustomersURL, { headers, body }, 'PUT')
    } else {
        if (!email && config.sendEventsFromAnonymousUsers === 'Only send events from users with emails') {
            return
        }
        const body = JSON.stringify({ name: event, data: properties })
        response = await fetchWithRetry(`${baseCustomersURL}/events`, { headers, body }, 'POST')
    }

    if (!statusOk(response)) {
        console.error(isIdentifyEvent ? `Unable to identify user ${email} in Customer.io` : `Unable to send event ${event} to Customer.io`)
    }
}

async function fetchWithRetry(url, options = {}, method = 'GET', isRetry = false) {
    try {
        const res = await fetch(url, { method: method, ...options })
        return res
    } catch {
        if (isRetry) {
            throw new Error(`${method} request to ${url} failed.`)
        }
        const res = await fetchWithRetry(url, options, (method = method), (isRetry = true))
        return res
    }
}

function statusOk(res) {
    return Math.floor(res.status / 100) === 2
}

function statusUnauthorized(res){
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
