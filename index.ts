import { RetryError } from '@posthog/plugin-scaffold'
import type { PluginInput, Meta, Plugin, PluginEvent } from '@posthog/plugin-scaffold'
import fetch from 'node-fetch'
import type { Response } from 'node-fetch'

const DEFAULT_HOST = 'track.customer.io'
const DEFAULT_SEND_EVENTS_FROM_ANONYMOUS_USERS = 'Send all events'

interface CustomerIoPluginInput extends PluginInput {
    config: {
        customerioSiteId: string
        customerioToken: string
        host?: 'track.customer.io' | 'track-eu.customer.io'
        sendEventsFromAnonymousUsers?:
            | 'Send all events'
            | 'Only send events from users with emails'
            | 'Only send events from users that have been identified'
        eventsToSend?: string
    }
    global: {
        authorizationHeader: string
        eventNames: string[]
        eventsConfig: EventsConfig
    }
}

enum EventsConfig {
    SEND_ALL = '1',
    SEND_EMAILS = '2',
    SEND_IDENTIFIED = '3'
}

const EVENTS_CONFIG_MAP = {
    'Send all events': EventsConfig.SEND_ALL,
    'Only send events from users with emails': EventsConfig.SEND_EMAILS,
    'Only send events from users that have been identified': EventsConfig.SEND_IDENTIFIED
}

export const setupPlugin: Plugin<CustomerIoPluginInput>['setupPlugin'] = async ({ config, global }) => {
    const customerioBase64AuthToken = Buffer.from(`${config.customerioSiteId}:${config.customerioToken}`).toString(
        'base64'
    )

    global.authorizationHeader = `Basic ${customerioBase64AuthToken}`

    const authResponse = await fetchWithRetry(
        'https://beta-api.customer.io/v1/api/info/ip_addresses',
        global.authorizationHeader
    )

    if (!authResponse || statusUnauthorized(authResponse)) {
        const authResponseJson = authResponse ? await authResponse.json() : null
        console.error(`Unable to connect to Customer.io - Response = ${authResponseJson || authResponse}`)
        return
    }

    if (!statusOk(authResponse)) {
        let response = await authResponse.json()
        console.error(`Service is down, retry later - Response = ${response}`)
        return
    }

    global.eventNames = config.eventsToSend ? config.eventsToSend.split(',').filter(Boolean) : []

    global.eventsConfig =
        EVENTS_CONFIG_MAP[config.sendEventsFromAnonymousUsers || DEFAULT_SEND_EVENTS_FROM_ANONYMOUS_USERS]
}

export const exportEvents: Plugin<CustomerIoPluginInput>['exportEvents'] = async (events, meta) => {
    const { global, config } = meta

    // KLUDGE: This shouldn't even run if setupPlugin failed. Needs to be fixed at the plugin server level
    if (!global.eventNames) {
        throw new RetryError('setupPlugin failed. Cannot run exportEvents.')
    }

    console.log(`Flushing batch of length ${events.length}`)
    for (const event of events) {
        if (
            (global.eventNames.length > 0 && !global.eventNames.includes(event.event)) ||
            (global.eventsConfig === EventsConfig.SEND_IDENTIFIED && isAnonymousUser(event))
        ) {
            continue
        }

        try {
            await exportToCustomerio(event, global.authorizationHeader, config.host || DEFAULT_HOST, meta)
        } catch (error) {
            console.error('Failed to export to Customer.io')
        }
    }
}

async function exportToCustomerio(
    payload: PluginEvent,
    authorizationHeader: string,
    customerioHost: string,
    { cache, global }: Meta<CustomerIoPluginInput>
) {
    const { event, distinct_id } = payload

    const properties = { ...payload.properties, ...(payload.properties?.$set || {}) }

    const baseCustomersURL = `https://${customerioHost}/api/v1/customers/${distinct_id}`
    const headers = { 'Content-Type': 'application/x-www-form-urlencoded', Authorization: authorizationHeader }

    let userExists = await cache.get(distinct_id, false)
    const isIdentifyEvent = event === '$identify'
    const email = isIdentifyEvent ? getEmailFromEvent(payload) : null

    const shouldCreateUserForConfig =
        global.eventsConfig === EventsConfig.SEND_ALL ||
        (global.eventsConfig === EventsConfig.SEND_EMAILS && email) ||
        (global.eventsConfig === EventsConfig.SEND_IDENTIFIED && isIdentifyEvent)

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

async function sendEventToCustomerIo(
    url: string,
    event: string,
    properties: Record<string, any>,
    headers: Record<string, any>
) {
    const body = JSON.stringify({ name: event, data: properties })
    const response = await fetchWithRetry(`${url}/events`, { headers, body }, 'POST')
    if (!response || !statusOk(response)) {
        console.error(`Unable to send event ${event} to Customer.io`)
    }
}

// Customer.io will just update the user and return `ok` if it already exists
async function createCustomerioUserIfNotExists(
    url: string,
    email: string | null,
    properties: Record<string, any>,
    headers: Record<string, any>
) {
    const body = JSON.stringify({ email, ...properties })
    try {
        const response = await fetchWithRetry(url, { headers, body }, 'PUT')
        if (!response || !statusOk(response)) {
            console.error(`Unable to create user with email ${email}. Status: ${response?.status}.`)
            return false
        }
        return true
    } catch (error) {
        console.error(error)
    }

    return false
}

async function fetchWithRetry(url: string, options = {}, method = 'GET', isRetry = false): Promise<Response | null> {
    try {
        const res = await fetch(url, { method: method, ...options })
        return res
    } catch {
        if (isRetry) {
            console.error(`${method} request to ${url} failed.`)
            return null
        }
        const res = await fetchWithRetry(url, options, (method = method), (isRetry = true))
        return res
    }
}

function statusOk(res: Response) {
    return res.status.toString().charAt(0) === '2'
}

function statusUnauthorized(res: Response) {
    return res.status == 401
}

function isEmail(email: string): boolean {
    if (typeof email !== 'string') {
        return false
    }
    const re = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
    return re.test(email.toLowerCase())
}

function isAnonymousUser({ distinct_id, properties }: PluginEvent) {
    if (properties) return properties['$device_id'] === distinct_id

    // A fallback in case the event doesn't have `properties` set, for some reason.
    const re = /^[\w]{14}-[\w]{14}-[\w]{8}-[\w]{6}-[\w]{14}$/g
    return re.test(String(distinct_id))
}

function getEmailFromKey(event: PluginEvent, key: '$set' | '$set_once' | 'properties'): string | null {
    const object = event[key]
    if (!object) {
        return null
    }
    if (!Object.keys(object).includes('email')) {
        return null
    }
    const email = object['email']
    return isEmail(email) ? email : null
}

function getEmailFromEvent(event: PluginEvent): string | null {
    if (isEmail(event.distinct_id)) {
        return event.distinct_id
    }

    return getEmailFromKey(event, '$set') || getEmailFromKey(event, '$set_once') || getEmailFromKey(event, 'properties')
}
