import http from 'k6/http';
import {
    check,
    fail
} from 'k6';
import {
    uuidv4
} from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Override with: k6 run -e BASE_URL=http://localhost:8080 -e STORE=k6 ...
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const STORE = __ENV.STORE || 'k6';

export const options = {
    vus: 10, // virtual users
    duration: '30s', // test duration
    thresholds: {
        http_req_duration: ['p(90)<500'], // 90% < 500ms
        // Fail the run when appends fail, not only when they are slow.
        checks: ['rate==1.0'],
        http_req_failed: ['rate==0'],
    },
};

const jsonHeaders = {
    headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
    },
};

// Creates the store the appends go to. A store left from an earlier run is reused.
export function setup() {
    const res = http.post(
        `${BASE_URL}/api/v1/stores`,
        JSON.stringify({ name: STORE }),
        Object.assign({}, jsonHeaders, { responseCallback: http.expectedStatuses(201, 409) }),
    );
    if (res.status !== 201 && res.status !== 409) {
        fail(`Could not create store '${STORE}': ${res.status} ${res.body}`);
    }
}

export default function() {
    const userId = uuidv4();

    // Appends a fact only if no fact carries its tag yet. Each tag is new, so every append
    // must succeed: the script measures conditional appends, not violated conditions.
    const payload = JSON.stringify({
        idempotencyKey: uuidv4(),
        condition: {
            type: 'tagQueryBased',
            failIfEventsMatch: {
                queryItems: [{
                    type: 'tagOnly',
                    tags: {
                        vu: userId
                    }
                }]
            },
            after: null
        },
        facts: [{
            type: 'UserCreated',
            subject: `user-${__VU}`,
            payload: {
                // "Hello world" base64-encoded
                data: 'SGVsbG8gd29ybGQ='
            },
            tags: {
                vu: userId
            }
        }]
    });

    const res = http.post(`${BASE_URL}/api/v1/stores/${STORE}/facts`, payload, jsonHeaders);
    check(res, {
        'status is 200': (r) => r.status === 200,
        'facts were appended': (r) => r.status === 200 && r.json('factIds').length === 1,
    });
}
