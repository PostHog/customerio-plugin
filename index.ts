import { CacheExtension, RetryError } from '@posthog/plugin-scaffold'
import type { PluginInput, Meta, Plugin, PluginEvent } from '@posthog/plugin-scaffold'
import fetch from 'node-fetch'
import { Response } from 'node-fetch'

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

const fetchWithErrorHandling: typeof fetch = async (url, init) => {
    let response: Response
    try {
        response = await fetch(url, init)
    } catch (e) {
        throw new RetryError(`Cannot reach the Customer.io API. ${e}`)
    }
    const responseStatusClass = Math.floor(response.status / 100)
    if (response.status === 401 || response.status === 403) {
        const responseData = await response.json()
        throw new Error(
            `Customer.io Site ID or API Key invalid! Response ${response.status}: ${JSON.stringify(responseData)}`
        )
    }
    if (response.status === 408 || response.status === 429 || responseStatusClass === 5) {
        const responseData = await response.json()
        throw new RetryError(
            `Received a potentially intermittent error from the Customer.io API. Response ${
                response.status
            }: ${JSON.stringify(responseData)}`
        )
    }
    if (responseStatusClass !== 2) {
        const responseData = await response.json()
        throw new Error(
            `Received an unexpected error from the Customer.io API. Response ${response.status}: ${JSON.stringify(
                responseData
            )}`
        )
    }
    return response
}

export const setupPlugin: Plugin<CustomerIoPluginInput>['setupPlugin'] = async ({ config, global }) => {
    const customerioBase64AuthToken = Buffer.from(`${config.customerioSiteId}:${config.customerioToken}`).toString(
        'base64'
    )
    global.authorizationHeader = `Basic ${customerioBase64AuthToken}`
    global.eventNames = config.eventsToSend ? config.eventsToSend.split(',').filter(Boolean) : []
    global.eventsConfig =
        EVENTS_CONFIG_MAP[config.sendEventsFromAnonymousUsers || DEFAULT_SEND_EVENTS_FROM_ANONYMOUS_USERS]

    await fetchWithErrorHandling('https://api.customer.io/v1/api/info/ip_addresses', {
        headers: { Authorization: global.authorizationHeader }
    })
    console.log('Successfully authenticated with Customer.io.')
}

export const exportEvents: Plugin<CustomerIoPluginInput>['exportEvents'] = async (events, meta) => {
    const { global, config } = meta
    // KLUDGE: This shouldn't even run if setupPlugin failed. Needs to be fixed at the plugin server level
    if (!global.eventNames) {
        throw new RetryError('Cannot run exportEvents because setupPlugin failed!')
    }
    const batchInfo = `Batch of ${events.length} event${events.length !== 1 ? 's' : ''} received.`
    if (events.length === 0) {
        console.log(`${batchInfo} Skipping.`)
        return
    }
    const filteredEvents = events.filter(
        (event) =>
            (global.eventNames.length === 0 || global.eventNames.includes(event.event)) &&
            (global.eventsConfig !== EventsConfig.SEND_IDENTIFIED || !isAnonymousUser(event))
    )
    if (filteredEvents.length === 0) {
        console.log(`${batchInfo} None passed filtering. Skipping.`)
        return
    } else {
        console.log(
            `${batchInfo} ${
                filteredEvents.length === events.length ? 'All' : filteredEvents.length
            } passed filtering. Proceeding...`
        )
    }
    await Promise.all(
        filteredEvents.map(
            async (event) =>
                await exportSingleEvent(event, global.authorizationHeader, config.host || DEFAULT_HOST, meta.cache)
        )
    )
    console.log(`Sent ${filteredEvents.length} event${filteredEvents.length !== 1 ? 's' : ''} to Customer.io.`)
}

async function exportSingleEvent(event: PluginEvent, authorizationHeader: string, host: string, cache: CacheExtension) {
    const combinedSetObject = { ...(event.$set_once || {}), ...(event.$set || {}) }
    const flattenedEventProperties = { ...(event.properties || {}), ...combinedSetObject }
    const email = getEmailFromEvent(event)

    const userExists = await cache.get(event.distinct_id, false)
    // See https://www.customer.io/docs/api/#operation/identify
    await fetchWithErrorHandling(`https://${host}/api/v1/customers/${event.distinct_id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', Authorization: authorizationHeader },
        body: JSON.stringify({ _update: userExists, email, identifier: event.distinct_id, ...combinedSetObject })
    })
    await cache.set(event.distinct_id, true)

    const eventType = event.event === '$pageview' ? 'page' : event.event === '$screen' ? 'screen' : 'event'
    const eventTimestamp = (event.timestamp ? new Date(event.timestamp).valueOf() : Date.now()) / 1000
    await fetchWithErrorHandling(`https://${host}/api/v1/customers/${event.distinct_id}/events`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: authorizationHeader },
        body: JSON.stringify({
            name: event.event,
            type: eventType,
            timestamp: eventTimestamp,
            data: flattenedEventProperties
        })
    })
}

function isAnonymousUser({ distinct_id, properties }: PluginEvent) {
    if (properties) return properties['$device_id'] === distinct_id

    // A fallback in case the event doesn't have `properties` set, for some reason.
    const re = /^[\w]{14}-[\w]{14}-[\w]{8}-[\w]{6}-[\w]{14}$/g
    return re.test(String(distinct_id))
}

function isEmail(email: string): boolean {
    if (typeof email !== 'string') {
        return false
    }
    const re = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
    return re.test(email.toLowerCase())
}

function getEmailFromSetObject(event: PluginEvent, key: '$set' | '$set_once' | 'properties'): string | null {
    const source = event[key]
    if (typeof source !== 'object' || !source['email']) {
        return null
    }
    const emailCandidate = source['email']
    return isEmail(emailCandidate) ? emailCandidate : null
}

function getEmailFromEvent(event: PluginEvent): string | null {
    if (isEmail(event.distinct_id)) {
        return event.distinct_id
    }
    return getEmailFromSetObject(event, '$set') || getEmailFromSetObject(event, '$set_once')
}
