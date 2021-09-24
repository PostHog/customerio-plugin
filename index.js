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

    const eventNames = (config.eventsToSend || '').split(',').filter(String)

    global.buffer = createBuffer({
        limit: (1 / 5) * 1024 * 1024, // 200kb
        timeoutSeconds: 5,
        onFlush: async (batch) => {
            console.log(`Flushing batch of length ${batch.length}`)
            for (const event of batch) {
                if (eventNames.length > 0 && !eventNames.includes(event.event)) { 
                    continue;
                }
                 await exportToCustomerio(event, global.customerioAuthHeader)
            }
        }
    })
}

export async function onEvent(event, { global }) {
    global.buffer.add(event)
}

async function exportToCustomerio(event, authHeader) {
    const eventInsertResponse = await fetchWithRetry(
        `https://track.customer.io/api/v1/customers/${event.distinct_id}/events`,
        {
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
                ...authHeader.headers
            },
            body: JSON.stringify({ name: event.event, data: event.properties })
        },
        'POST'
    )

    if (!statusOk(eventInsertResponse)) {
        console.log(`Unable to send event ${event.event} to Customer.io`)
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
