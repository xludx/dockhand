/**
 * SSE endpoint for hawser agent reconnection events.
 *
 * When a hawser agent replaces its connection (e.g. after containers are
 * recreated), this stream fires a `hawser_reconnect` event so the frontend
 * can clear its stale container ID cache and re-fetch fresh data before
 * starting stats polling. Without this, the frontend requests stats for
 * old container IDs, the requests fail, and the connection drops in a loop.
 */
import type { RequestHandler } from './$types';
import { containerEventEmitter } from '$lib/server/event-collector';
import { authorize } from '$lib/server/authorize';
import { json } from '@sveltejs/kit';

export const GET: RequestHandler = async ({ url, cookies }) => {
	const auth = await authorize(cookies);

	if (auth.authEnabled && !await auth.can('containers', 'view')) {
		return json({ error: 'Permission denied' }, { status: 403 });
	}

	const envIdParam = url.searchParams.get('env');
	const filterEnvId = envIdParam ? parseInt(envIdParam) : null;

	let heartbeatInterval: ReturnType<typeof setInterval>;
	let handleReconnect: ((payload: { environmentId: number }) => void) | null = null;

	const stream = new ReadableStream({
		start(controller) {
			const encoder = new TextEncoder();

			const sendEvent = (type: string, data: any) => {
				try {
					controller.enqueue(encoder.encode(`event: ${type}\ndata: ${JSON.stringify(data)}\n\n`));
				} catch {
					// Ignore — client disconnected
				}
			};

			sendEvent('connected', { timestamp: new Date().toISOString() });

			// Keep-alive: every 5s (prevents proxy idle timeouts)
			heartbeatInterval = setInterval(() => {
				try {
					sendEvent('heartbeat', { timestamp: new Date().toISOString() });
				} catch {
					clearInterval(heartbeatInterval);
				}
			}, 5000);

			handleReconnect = (payload: { environmentId: number }) => {
				// If a specific env was requested, only forward events for that env.
				// Otherwise forward all reconnection events.
				if (filterEnvId === null || payload.environmentId === filterEnvId) {
					sendEvent('hawser_reconnect', payload);
				}
			};

			containerEventEmitter.on('hawser_reconnect', handleReconnect);
		},
		cancel() {
			clearInterval(heartbeatInterval);
			if (handleReconnect) {
				containerEventEmitter.off('hawser_reconnect', handleReconnect);
				handleReconnect = null;
			}
		}
	});

	return new Response(stream, {
		headers: {
			'Content-Type': 'text/event-stream',
			'Cache-Control': 'no-cache',
			'Connection': 'keep-alive',
			'X-Accel-Buffering': 'no'
		}
	});
};
