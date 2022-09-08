import { RetryError, StorageExtension } from '@posthog/plugin-scaffold'
import type { PluginInput, Plugin, ProcessedPluginEvent } from '@posthog/plugin-scaffold'
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

interface Customer {
    status: Set<'seen' | 'identified' | 'with_email'>
    existsAlready: boolean
    email: string | null
}

async function callCustomerIoApi(
    method: NonNullable<RequestInit['method']>,
    host: string,
    path: string,
    authorization: string,
    body?: any
) {
    const headers: Record<string, any> = { 'User-Agent': 'PostHog Customer.io App', Authorization: authorization }
    let bodySerialized: string | undefined
    if (body != null) {
        headers['Content-Type'] = 'application/json'
        bodySerialized = JSON.stringify(body)
    }
    let response: Response
    try {
        response = await fetch(`https://${host}${path}`, { method, headers, body: bodySerialized })
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

export const setupPlugin: Plugin<CustomerIoPluginInput>['setupPlugin'] = async ({ config, global, storage }) => {
    const customerioBase64AuthToken = Buffer.from(`${config.customerioSiteId}:${config.customerioToken}`).toString(
        'base64'
    )
    global.authorizationHeader = `Basic ${customerioBase64AuthToken}`
    global.eventNames = config.eventsToSend ? config.eventsToSend.split(',').filter(Boolean) : []
    global.eventsConfig =
        EVENTS_CONFIG_MAP[config.sendEventsFromAnonymousUsers || DEFAULT_SEND_EVENTS_FROM_ANONYMOUS_USERS]

    const credentialsVerifiedPreviously = await storage.get(global.authorizationHeader, false)

    if (credentialsVerifiedPreviously) {
        console.log('Customer.io credentials verified previously. Completing setupPlugin.')
        return
    }

    // See https://www.customer.io/docs/api/#operation/getCioAllowlist
    await callCustomerIoApi('GET', 'api.customer.io', '/v1/api/info/ip_addresses', global.authorizationHeader)
    await storage.set(global.authorizationHeader, true)
    console.log('Successfully authenticated with Customer.io. Completing setupPlugin.')
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
    const filteredWithCustomers: [ProcessedPluginEvent, Customer][] = await Promise.all(
        events.map(async (event) => [event, await syncCustomerMetadata(event, meta.storage)])
    )
    const nameFilteredEventsWithCustomers = filteredWithCustomers.filter(
        (event) => global.eventNames.length === 0 || global.eventNames.includes(event[0].event)
    )
    const filterCreateAlias = nameFilteredEventsWithCustomers.filter((event) => event[0].event !== '$create_alias')
    const fullyFilteredEventsWithCustomers = filterCreateAlias.filter(([, customer]) =>
        shouldCustomerBeTracked(customer, global.eventsConfig)
    )

    const finalEventCount = fullyFilteredEventsWithCustomers.length
    if (finalEventCount === 0) {
        console.log(`${batchInfo} None passed filtering. Skipping.`)
        return
    } else {
        console.log(
            `${batchInfo} ${
                finalEventCount === events.length ? 'All' : finalEventCount
            } passed filtering. Proceeding...`
        )
    }

    // Tracking events in two stages, to improve consistency of customer creation
    const customerCreateEvents = fullyFilteredEventsWithCustomers.filter(([, customer]) => !customer.existsAlready)
    const customerUpdateEvents = fullyFilteredEventsWithCustomers.filter(([, customer]) => customer.existsAlready)
    await Promise.all(
        customerCreateEvents.map(
            async ([event, customer]) =>
                await exportSingleEvent(event, customer, global.authorizationHeader, config.host || DEFAULT_HOST)
        )
    )
    await Promise.all(
        customerUpdateEvents.map(
            async ([event, customer]) =>
                await exportSingleEvent(event, customer, global.authorizationHeader, config.host || DEFAULT_HOST)
        )
    )

    console.log(`Sent ${finalEventCount} event${finalEventCount !== 1 ? 's' : ''} to Customer.io.`)
}

async function syncCustomerMetadata(event: ProcessedPluginEvent, storage: StorageExtension): Promise<Customer> {
    const customerStatusKey = `customer-status/${event.distinct_id}`
    const customerStatusArray = (await storage.get(customerStatusKey, [])) as string[]
    const customerStatus = new Set(customerStatusArray) as Customer['status']
    const customerExistsAlready = customerStatus.has('seen')
    const email = getEmailFromEvent(event)

    // Update customer status
    customerStatus.add('seen')
    if (event.event === '$identify') {
        customerStatus.add('identified')
    }
    if (email) {
        customerStatus.add('with_email')
    }

    if (customerStatus.size > customerStatusArray.length) {
        await storage.set(customerStatusKey, Array.from(customerStatus))
    }

    return {
        status: customerStatus,
        existsAlready: customerExistsAlready,
        email
    }
}

function shouldCustomerBeTracked(customer: Customer, eventsConfig: EventsConfig): boolean {
    switch (eventsConfig) {
        case EventsConfig.SEND_ALL:
            return true
        case EventsConfig.SEND_EMAILS:
            return customer.status.has('with_email')
        case EventsConfig.SEND_IDENTIFIED:
            return customer.status.has('identified')
        default:
            throw new Error(`Unknown eventsConfig: ${eventsConfig}`)
    }
}

async function exportSingleEvent(
    event: ProcessedPluginEvent,
    customer: Customer,
    authorizationHeader: string,
    host: string
) {
    // Clean up properties
    if (event.properties) {
        delete event.properties['$set']
        delete event.properties['$set_once']
    }

    const customerPayload: Record<string, any> = {
        ...(event.$set || {}),
        _update: customer.existsAlready,
        identifier: event.distinct_id
    }
    if (customer.email) {
        customerPayload.email = customer.email
    }
    // Create or update customer
    // See https://www.customer.io/docs/api/#operation/identify
    await callCustomerIoApi('PUT', host, `/api/v1/customers/${event.distinct_id}`, authorizationHeader, customerPayload)

    const eventType = event.event === '$pageview' ? 'page' : event.event === '$screen' ? 'screen' : 'event'
    const eventTimestamp = (event.timestamp ? new Date(event.timestamp).valueOf() : Date.now()) / 1000
    // Track event
    // See https://www.customer.io/docs/api/#operation/track
    await callCustomerIoApi('POST', host, `/api/v1/customers/${event.distinct_id}/events`, authorizationHeader, {
        name: event.event,
        type: eventType,
        timestamp: eventTimestamp,
        data: event.properties || {}
    })
}

function isEmail(email: string): boolean {
    if (typeof email !== 'string') {
        return false
    }
    const re = /^(([^<>()[\]\\.,;:\s@"]+(\.[^<>()[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/
    return re.test(email.toLowerCase())
}

function getEmailFromEvent(event: ProcessedPluginEvent): string | null {
    const setAttribute = event.$set
    if (typeof setAttribute !== 'object' || !setAttribute['email']) {
        return null
    }
    const emailCandidate = setAttribute['email']
    if (isEmail(emailCandidate)) {
        return emailCandidate
    }
    // Use distinct ID as a last resort
    if (isEmail(event.distinct_id)) {
        return event.distinct_id
    }
    return null
}
