import http from 'k6/http';
import {
    check,
    sleep
} from 'k6';
import {
    uuidv4
} from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export let options = {
    vus: 10, // virtual users
    duration: '30s', // test duration
    thresholds: {
        http_req_duration: ['p(90)<500'], // 90% < 500ms
    },
};

export default function() {
    const url = 'http://localhost:8080/api/v1/stores/my-fact-store/facts/append';

    const userId = uuidv4();

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
            id: uuidv4(),
            type: 'UserCreated',
            subjectRef: {
                type: 'user',
                id: `user-${__VU}`,
            },
            payload: {
                // "Hello world" base64-encoded
                data: 'SGVsbG8gd29ybGQ='
            },
            metadata: {},
            tags: {
                vu: userId
            }
        }]
    });

    const params = {
        headers: {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        },
    };

    const res = http.post(url, payload, params);
    check(res, {
        'status is 200': (r) => r.status === 200,
    });

}