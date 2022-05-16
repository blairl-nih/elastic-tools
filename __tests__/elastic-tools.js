const elasticsearch         = require('@elastic/elasticsearch');
const moment                = require('moment');
const nock                  = require('nock');
const path                  = require('path');
const winston               = require('winston');
const WinstonNullTransport  = require('winston-null-transport');

const ElasticTools = require('../index');

const logger = winston.createLogger({
    level: 'debug',
    format: winston.format.simple(),
    transports: [
        new WinstonNullTransport()
    ]
});

/*
    Some long-running tests are particularly annoying to run under normal
    circumstances, but should always run in GitHub Actions.  Replacing it()
    with testOnGithubIt() provides a convenient way to accomplish this goal.
*/
const ON_GITHUB = process.env.GITHUB_ACTIONS !== undefined;
const testOnGithubIt = !!ON_GITHUB ? it : it.skip;


beforeEach(() => {

    // Each time a new client is created, before the first query executes, the client checks that it's connected to Elasticsearch.
    const scope = nock('http://example.org:9200')
    .get('/')
    .reply(200, {
        "name": "5fdf33196b0f",
        "cluster_name": "docker-cluster",
        "cluster_uuid": "A7Fv9yZRT_u4Wf8cJwEYXg",
        "version": {
            "number": "7.9.2",
            "build_flavor": "default",
            "build_type": "docker",
            "build_hash": "d34da0ea4a966c4e49417f2da2f244e3e97b4e6e",
            "build_date": "2020-09-23T00:45:33.626720Z",
            "build_snapshot": false,
            "lucene_version": "8.6.2",
            "minimum_wire_compatibility_version": "6.8.0",
            "minimum_index_compatibility_version": "6.0.0-beta1"
        },
        "tagline": "You Know, for Search"
    });
});

beforeAll(() => {
    nock.disableNetConnect();
});

//After each test, cleanup any remaining mocks
afterEach(() => {
    nock.cleanAll();
});

afterAll(() => {
    nock.enableNetConnect();
});

describe('ElasticTools', () => {

    describe('createTimestampedIndex', () => {

        it('creates the index', async () => {
            const mappings = require(path.join(__dirname, 'data', '/mappings.json'));
            const settings = require(path.join(__dirname, 'data', 'settings.json'));

            const aliasName = 'bryantestidx';
            const urlRegex = /\/(.*)/;
            let interceptedIdx = '';

            const scope = nock('http://example.org:9200')
            .put(
                (uri) => {
                    //So we need to get the index name the function created, and in elasticsearch,
                    //that is the URI
                    const match = uri.match(urlRegex);
                    if (match) {
                        interceptedIdx = match[1];
                        return true;
                    } else {
                        return false;
                    }
                },
                {
                    settings: settings.settings,
                    mappings: mappings.mappings
                }
            )
            .reply(200, {"acknowledged":true,"shards_acknowledged":true,"index":interceptedIdx} );

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            const expectedTime = Date.now();
            const indexName = await estools.createTimestampedIndex(aliasName, mappings, settings);

            //Let's make sure the nock call was called
            expect(scope.isDone()).toBeTruthy();

            const dateRegex = /^bryantestidx_([\d]{4})([\d]{2})([\d]{2})_([\d]{2})([\d]{2})([\d]{2})$/;
            const matches = indexName.match(dateRegex);

            //Now the index name should match
            expect(matches).toBeTruthy();

            const [year, mon, day, hr, min, sec] = matches.slice(1).map(s => Number.parseInt(s));

            const actualTime = new Date(year, mon-1, day, hr, min, sec).getTime();

            //So the expected will never be the same as the actual time, so
            //lets just make sure it is within 5 seconds +/-
            expect(actualTime).toBeGreaterThanOrEqual(expectedTime - 5000);
            expect(actualTime).toBeLessThan(expectedTime + 5000);
        })

    });

    describe('createIndex', () => {

        it('creates the index', async () => {

            const now = moment();
            const timestamp = now.format("YYYYMMDD_HHmmss");
            const indexName = 'bryantestidx' + timestamp;

            const mappings = require(path.join(__dirname, 'data', '/mappings.json'));
            const settings = require(path.join(__dirname, 'data', 'settings.json'));

            const scope = nock('http://example.org:9200')
                .put(`/${indexName}`, {
                    settings: settings.settings,
                    mappings: mappings.mappings
                })
                .reply(200, {"acknowledged":true,"shards_acknowledged":true,"index":indexName} );

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.createIndex(indexName, mappings, settings);

            expect(scope.isDone()).toBeTruthy();
        });

        it('handles index already exists', async() => {
            const now = moment();
            const timestamp = now.format("YYYYMMDD_HHmmss");
            const indexName = 'bryantestidx' + timestamp;

            const mappings = require(path.join(__dirname, 'data', '/mappings.json'));
            const settings = require(path.join(__dirname, 'data', 'settings.json'));

            const scope = nock('http://example.org:9200')
                .put(`/${indexName}`, {
                    settings: settings.settings,
                    mappings: mappings.mappings
                })
                .reply(400,
                    {
                        "error": {
                        "root_cause": [
                        {
                        "type": "index_already_exists_exception",
                        "reason": `index [${indexName}/EE0VmcmPT-q9MatbvD6vAw] already exists`,
                        "index_uuid": "EE0VmcmPT-q9MatbvD6vAw",
                        "index": indexName
                        }
                        ],
                        "type": "index_already_exists_exception",
                        "reason": `index [${indexName}/EE0VmcmPT-q9MatbvD6vAw] already exists`,
                        "index_uuid": "EE0VmcmPT-q9MatbvD6vAw",
                        "index": indexName
                        },
                        "status": 400
                    }
                );

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            //TODO: Actually check and see if we get an logged error when the
            //exception occurs.
            try {
                await estools.createIndex(indexName, mappings, settings);
            } catch (err) {
                expect(err).not.toBeNull();
            }

            expect(scope.isDone()).toBeTruthy();
        });

    });

    describe('optimizeIndex', () => {
        it("optimizes the index", async() => {
            const indexName = 'bryantestidx';

            const scope = nock('http://example.org:9200')
                .post(`/${indexName}/_forcemerge?max_num_segments=1`, body => true)
                .reply(200, {"_shards":{"total":2,"successful":1,"failed":0}});

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.optimizeIndex(indexName);

            expect(scope.isDone()).toBeTruthy();
        });

        it("handles a 504 response", async () => {

        });

        it("logs an error on server error", async () => {

        });


        //The following tests simulate slow responses, something we have seen with Elasticsearch.
        //So we increase the timeout to 1.5 minutes for this request. These need to be tested upon change,
        //but it would slow down regular tests, so we use testOnGithubIt() to make it conditional.
        testOnGithubIt("optimizes the index when processing time approaches the timeout", async() => {
            const indexName = 'bryantestidx';

            const scope = nock('http://example.org:9200')
                .post(`/${indexName}/_forcemerge?max_num_segments=1`, body => true)
                .delay({
                    // Simulate the server found, and a long-running request, but shorter than timeout.
                    body: 89000
                })
                .reply(200, {"_shards":{"total":2,"successful":1,"failed":0}});


            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.optimizeIndex(indexName);

            expect(scope.isDone()).toBeTruthy();
        }, 100000);

        testOnGithubIt("doesn't time out for a long-running request when the server is found.", async() => {
            const indexName = 'bryantestidx';

            const scope = nock('http://example.org:9200')
                .post( url => true, body => true)
                .delay({
                    // Simulate the server found, and a long-running request.
                    body: 95000
                })
                .reply(200, {"_shards":{"total":2,"successful":1,"failed":0}});

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200',
                maxRetries: 0   // Elasticsearch will retry, but scope is single use. Alternatively, .persist() the scope
            });

            const estools = new ElasticTools(logger, client);

            try {
                await estools.optimizeIndex(indexName);
            } catch (err) {
                // This assertion should never be hit.
                expect(err).not.toBeNull();
            }

            expect(scope.isDone()).toBeTruthy();
        }, 110000);

        /**
         * This test is deliberately disabled due to a nuanced bug we've decided to ignore for now.
         * In short, if the connection timeout limit is reached, Elasticsearch returns an error, but
         * doesn't actually close the connection. According to Elasticsearch documentation, this is
         * "normal" for node applications. This being a rare circumstance, we've decided to punt on
         * the issue for now and may revisit it at a later time.
         */
        // testOnGithubIt("throws a timeout error when the server doesn't exist/doesn't respond.", async () => {
        //     const indexName = 'bryantestidx';

        //     const scope = nock('http://example.org:9200')
        //         .post(url => true, body => true)
        //         .delay({
        //             // Simulate the server not being found/not responding.
        //             head: 95000
        //         })
        //         .reply(200, { "_shards": { "total": 2, "successful": 1, "failed": 0 } });

        //     const client = new elasticsearch.Client({
        //         node: 'http://example.org:9200',
        //         maxRetries: 0   // Elasticsearch will retry, but scope is single use. Alternatively, .persist() the scope
        //     });

        //     const estools = new ElasticTools(logger, client);

        //     expect.assertions(2);
        //     try {
        //         await estools.optimizeIndex(indexName);
        //         console.log("Continued on!")
        //         debugger;
        //     } catch (err) {
        //         expect(err.name).toBe('TimeoutError');
        //         debugger;
        //     }

        //     expect(scope.isDone()).toBeTruthy();

        // }, 110000);

    });

    describe('getIndicesOlderThan', () => {
        it('returns 1 when 1 is old', async() => {
            const indexName = 'bryantestidx';

            const scope = nock('http://example.org:9200')
                .get(`/${indexName}*/_settings/index.creation_date`)
                .reply(200, {
                    "bryantestidx_1":{ "settings": {"index": {"creation_date":"1523901276157"}}}
                });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            const expected = ["bryantestidx_1"];
            const indices = await estools.getIndicesOlderThan(indexName, 1525225677000);

            expect(indices).toEqual(expected);
            expect(scope.isDone()).toBeTruthy();
        });

        it('returns 0 when 1 not old', async() => {
            const indexName = 'bryantestidx';

            const scope = nock('http://example.org:9200')
                .get(`/${indexName}*/_settings/index.creation_date`)
                .reply(200, {
                    "bryantestidx_1":{ "settings": {"index": {"creation_date":"1525225677001"}}}
                });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            const expected = [];
            const indices = await estools.getIndicesOlderThan(indexName, 1525225677000);

            expect(indices).toEqual(expected);
            expect(scope.isDone()).toBeTruthy();
        });

        it('returns 2 in order when 2 of 3 are old', async() => {
            const indexName = 'bryantestidx';

            const scope = nock('http://example.org:9200')
                .get(`/${indexName}*/_settings/index.creation_date`)
                .reply(200, {
                    "bryantestidx_1":{ "settings": {"index": {"creation_date":"1525225677001"}}},
                    "bryantestidx_2":{ "settings": {"index": {"creation_date":"1525225676001"}}},
                    "bryantestidx_3":{ "settings": {"index": {"creation_date":"1525225676002"}}}
                });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            const expected = ['bryantestidx_3','bryantestidx_2'];
            const indices = await estools.getIndicesOlderThan(indexName, 1525225677000);

            expect(indices).toEqual(expected);
            expect(scope.isDone()).toBeTruthy();
        });

        it('handles server error', async () => {
            const indexName = 'bryantestidx';

            const scope = nock('http://example.org:9200')
            .get(`/${indexName}*/_settings/index.creation_date`)
            .reply(500);

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            expect.assertions(2);
            try {
                const indices = await estools.getIndicesOlderThan(indexName, 1525225677000);
            } catch (err) {
                expect(err).toBeTruthy();
            }

            expect(scope.isDone()).toBeTruthy();
        });

        it('checks alias name', async () => {

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);
            try {
                const indices = await estools.getIndicesOlderThan();
            } catch (err) {
                expect(err).toMatchObject(
                    { message: "aliasName cannot be null" }
                );
            }
        });

    })


    describe('setAliasToSingleIndex', () => {

        const aliasName = 'bryantestidx';

        it ('adds without removing', async () => {

            const indexName = aliasName + "_1";

            //Setup nocks
            const scope = nock('http://example.org:9200');

            //Get Indices for Alias, not finding any
            scope.get(`/_alias/${aliasName}`)
                .reply(404, {
                    "error": `alias [${aliasName}] missing`,
                    "status": 404
                });
            //Update Alias
            scope.post(`/_aliases`, {
                    actions: [
                        { add: { indices: indexName, alias: aliasName } }
                    ]
                })
                .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.setAliasToSingleIndex(aliasName, indexName);

            expect(nock.isDone()).toBeTruthy();
        })

        it ('add one and removes one', async () => {
            const indexName = aliasName + "_1";
            const removeIndex = aliasName + "_2";
            //Setup nocks
            const scope = nock('http://example.org:9200');

            //Get Indices for Alias, not finding any
            // need nock for getIndicesForAlias
            scope.get(`/_alias/${aliasName}`)
                .reply(200, {
                    [removeIndex]: {
                        "aliases": {
                            [aliasName]: {}
                        }
                    }
                });

            //Update Alias
            scope.post(`/_aliases`, {
                    actions: [
                        { add: { indices: indexName, alias: aliasName } },
                        { remove: { indices: [ removeIndex ], alias: aliasName } }
                    ]
                })
                .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.setAliasToSingleIndex(aliasName, indexName);

            expect(nock.isDone()).toBeTruthy();
        })

    });

    describe('updateAlias', () => {
        //Gonna use this a lot here, so set it once
        const aliasName = 'bryantestidx';

        it('checks for at least one add or remove', async ()=> {

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            //No params
            try {
                await estools.updateAlias(aliasName);
            } catch(err) {
                expect(err).toMatchObject({
                    message: "You must add or remove at least one index"
                });
            }

            //One is not a string or array
            try {
                await estools.updateAlias(aliasName, { add: 1});
            } catch(err) {
                expect(err).toMatchObject({
                    message: "Indices to add must either be a string or an array of items"
                });
            }

            //One is not a string or array
            try {
                await estools.updateAlias(aliasName, { remove: 1});
            } catch(err) {
                expect(err).toMatchObject({
                    message: "Indices to remove must either be a string or an array of items"
                });
            }

        })

        it('adds one', async() => {

            const index = "myindex";

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices: index, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.updateAlias(aliasName, { add: index});

            expect(nock.isDone()).toBeTruthy();

        })

        it('adds one arr', async() => {

            const index = [ "myindex" ];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices: index, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.updateAlias(aliasName, { add: index});

            expect(nock.isDone()).toBeTruthy();

        })

        it('removes one arr', async() => {
            const index = [ "myindex" ];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { remove: { indices: index, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.updateAlias(aliasName, { remove: index});

            expect(nock.isDone()).toBeTruthy();
        })

        it('swaps one for one', async() => {
            const add = "myindex";
            const remove = "myindex3";

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices: add, alias: aliasName } },
                    { remove: { indices: remove, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.updateAlias(aliasName, { add, remove });

            expect(nock.isDone()).toBeTruthy();
        })

        it('adds many', async() => {
            const indices = ["myindex", "myindex2"];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.updateAlias(aliasName, { add: indices});

            expect(nock.isDone()).toBeTruthy();

        })

        it('removes many', async() => {
            const indices = ["myindex", "myindex2"];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { remove: { indices, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.updateAlias(aliasName, { remove: indices});

            expect(nock.isDone()).toBeTruthy();
        })

        it('swaps many for many', async() => {
            const add = ["myindex", "myindex2"];
            const remove = ["myindex3", "myindex4"];

            const scope = nock('http://example.org:9200')
            .post(`/_aliases`, {
                actions: [
                    { add: { indices: add, alias: aliasName } },
                    { remove: { indices: remove, alias: aliasName } }
                ]
            })
            .reply(200, { "acknowledged": true });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.updateAlias(aliasName, { add, remove });

            expect(nock.isDone()).toBeTruthy();
        })

        it('handles server error', async () => {

        });
    })

    describe('getIndicesForAlias', () => {
        ///_alias/<%=name%>

        it('returns indices', async () => {
            const now = Date.now();

            //Set the prefix
            const aliasName = 'bryantestidx';
            const indexName = aliasName + "_1";

            //This is the nock for getIndicesOlderThan
            //If the creation date is now, then there is nothing older.
            const scope = nock('http://example.org:9200')
                .get(`/_alias/${aliasName}`)
                .reply(200, {
                    [indexName]: {
                        "aliases": {
                           [aliasName]: {}
                        }
                    }
                });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            const expectedIndices = [indexName];
            const actualIndices = await estools.getIndicesForAlias(aliasName);

            //Check that all of our expected calls are done
            expect(nock.isDone()).toBeTruthy();

            //Check the data is as expected.
            expect(actualIndices).toEqual(expectedIndices);
        })

        if ('handles 404', async () => {
            const aliasName = 'bryantestidx';

            const scope = nock('http://example.org:9200')
                .get(`/_alias/${aliasName}`)
                .reply(404, {
                    "error": `alias [${aliasName}] missing`,
                    "status": 404
                });

                const client = new elasticsearch.Client({
                    node: 'http://example.org:9200'
                });

            const estools = new ElasticTools(logger, client);

            const expectedIndices = [];
            const actualIndices = await estools.getIndicesForAlias(aliasName);

            //Check that all of our expected calls are done
            expect(nock.isDone()).toBeTruthy();

            //Check the data is as expected.
            expect(actualIndices).toEqual(expectedIndices);
        });

        if ('handles Exception', async () => {
            const aliasName = 'bryantestidx';
            const scope = nock('http://example.org:9200')
                .get(`/_alias/${aliasName}`)
                .reply(500);

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            const expectedIndices = [];
            expect.assertions(1);
            try {
                const actualIndices = await estools.getIndicesForAlias(aliasName);
            } catch (err) {
                expect(err).toBeTruthy();
            }

            expect(nock.isDone()).toBeTruthy();
        });
    })

    describe('indexDocument', () => {

        const successfulCreateResponse = '{"_index":"foo","_type":"_doc","_id":"72","_version":1,"result":"created","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":1,"_primary_term":1}';
        const successfulUpdateResponse = '{"_index":"foo","_type":"_doc","_id":"72","_version":2,"result":"updated","_shards":{"total":2,"successful":1,"failed":0},"_seq_no":2,"_primary_term":1}';

        it.each([
            [
                '(creates) without issue',
                72,
                { "username": "alice", "message": "chirp"},
                successfulCreateResponse
            ],
            [
                '(updates) without issue',
                72,
                { "username": "alice", "message": "chirp" },
                successfulUpdateResponse
            ]
        ])(
            'indexes %s',
            async(name, docID, docBody, response ) => {
                const scope = nock('http://example.org:9200')
                    .put(`/twitter/_doc/${docID}`, (body) => {
                        return JSON.stringify(body) === JSON.stringify(docBody)
                    })
                    .reply(200, response)

                const client = new elasticsearch.Client({
                    node: 'http://example.org:9200'
                });

                const estools = new ElasticTools(logger, client);

                await estools.indexDocument("twitter", docID, docBody);

                expect(nock.isDone()).toBeTruthy();
            }
        );

    });

    describe('indexDocumentBulk', () => {

        const goodCreateResponse = {
            "took": 11,
            "errors": false,
            "items": [
                {
                    "index": {
                        "_index": "twitter",
                        "_id": "11",
                        "_version": 1,
                        "result": "created",
                        "_shards": {
                            "total": 3,
                            "successful": 1,
                            "failed": 0
                        },
                        "created": true,
                        "status": 201
                    }
                }
            ]
        }

        const goodUpdateResponse = {
            "took": 13,
            "errors": false,
            "items": [
                {
                    "index": {
                        "_index": "twitter",
                        "_id": "11",
                        "_version": 2,
                        "result": "updated",
                        "_shards": {
                            "total": 3,
                            "successful": 1,
                            "failed": 0
                        },
                        "created": false,
                        "status": 200
                    }
                }
            ]
        }

        const goodCreateUpdateResponse = {
            "took": 8,
            "errors": false,
            "items": [
                {
                    "index": {
                        "_index": "twitter",
                        "_id": "12",
                        "_version": 1,
                        "result": "created",
                        "_shards": {
                            "total": 3,
                            "successful": 1,
                            "failed": 0
                        },
                        "created": true,
                        "status": 201
                    }
                },
                {
                    "index": {
                        "_index": "twitter",
                        "_id": "12",
                        "_version": 2,
                        "result": "updated",
                        "_shards": {
                            "total": 3,
                            "successful": 1,
                            "failed": 0
                        },
                        "created": false,
                        "status": 200
                    }
                }
            ]
        }

        const errorResponse = {
            "took": 44,
            "errors": true,
            "items": [
                {
                    "index": {
                        "_index": "twitter",
                        "_id": "11",
                        "status": 400,
                        "error": {
                            "type": "mapper_parsing_exception",
                            "reason": "failed to parse",
                            "caused_by": {
                                "type": "json_parse_exception",
                                "reason": "Unrecognized token 'tweettweet': was expecting 'null', 'true', 'false' or NaN\n at [Source: org.elasticsearch.common.bytes.BytesReference$MarkSupportingStreamInputWrapper@51d9914e; line: 1, column: 44]"
                            }
                        }
                    }
                }
            ]
        }

        const num11Req = '{"index":{"_index":"twitter","_id":"11"}}\n' +
                         '{"username":"bob","message":"tweettweet"}\n';

        const num12Req = '{"index":{"_index":"twitter","_id":"12"}}\n' +
                         '{"username":"bob","message":"tweettweet"}\n' +
                         '{"index":{"_index":"twitter","_id":"12"}}\n' +
                         '{"username":"bob","message":"tweettweet"}\n';

        it.each([
            [
                '(creates) without issue',
                [ [ "11", { "username": "bob", "message": "tweettweet" } ] ],
                num11Req,
                goodCreateResponse,
                {
                    created: ['11'],
                    updated: [],
                    errors: []
                }
            ],
            [
                '(updates) without issue',
                [ [ "11", { "username": "bob", "message": "tweettweet" } ] ],
                num11Req,
                goodUpdateResponse,
                {
                    created: [],
                    updated: ['11'],
                    errors: []
                }
            ],
            [
                'creates than updates without issue',
                [
                    [ "12", { "username": "bob", "message": "tweettweet" } ],
                    [ "12", { "username": "bob", "message": "tweettweet" } ]
                ],
                num12Req,
                goodCreateUpdateResponse,
                {
                    created: ['12'],
                    updated: ['12'],
                    errors: []
                }
            ],
            [
                'has errors in documents',
                //yeah, this looks valid... we are still gonna send back an error. :)
                [ [ "11", { "username": "bob", "message": "tweettweet" } ] ],
                num11Req,
                errorResponse,
                {
                    created: [],
                    updated: [],
                    errors: [{
                        id: "11",
                        "error": {
                            "type": "mapper_parsing_exception",
                            "reason": "failed to parse",
                            "caused_by": {
                                "type": "json_parse_exception",
                                "reason": "Unrecognized token 'tweettweet': was expecting 'null', 'true', 'false' or NaN\n at [Source: org.elasticsearch.common.bytes.BytesReference$MarkSupportingStreamInputWrapper@51d9914e; line: 1, column: 44]"
                            }
                        }
                    }]
                }
            ]//,
            //[
            //    'does everything',
            //]
        ])(
            'indexes %s',
            async (name, docArr, reqbody, response, expected) => {
                const scope = nock('http://example.org:9200')
                .post(`/_bulk`, (body) => {
                    return body === reqbody;
                })
                .reply(200, response);

                const client = new elasticsearch.Client({
                    node: 'http://example.org:9200'
                });

                const estools = new ElasticTools(logger, client);

                const actual = await estools.indexDocumentBulk("twitter", docArr);
                expect(actual).toEqual(expected)

                expect(nock.isDone()).toBeTruthy();
            }
        )

        it('throws on server error', async () => {
            const scope = nock('http://example.org:9200')
            .post('/_bulk', (body) => {
                return true
            })
            .reply(500);

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            expect.assertions(4);
            try {
                const actual = await estools.indexDocumentBulk(
                    "twitter",
                    [ [ "11", { "username": "bob", "message": "tweettweet" } ] ]
                );
            } catch (err) {
                expect(err.name).toBe('ResponseError');
                expect(err.statusCode).toBe(500);
                expect(err.body).toBe('');
            }

            expect(nock.isDone()).toBeTruthy();
        })

    });

    describe('deleteIndex', () => {

        it('deletes the index', async () => {
            const aliasName = 'bryantestidx';

            //If the creation date is now, then there is nothing older.
            const scope = nock('http://example.org:9200')
                .delete(`/${aliasName}_2`)
                .reply(200, {
                    "acknowledged": true
                })

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.deleteIndex(aliasName + "_2");

            expect(nock.isDone()).toBeTruthy();

        })

    });


    describe('cleanupOldIndices', () => {
        it ('has nothing to clean', async () => {
            const now = Date.now();

            //Set the prefix
            const aliasName = 'bryantestidx';

            //This is the nock for getIndicesOlderThan
            //If the creation date is now, then there is nothing older.
            const scope = nock('http://example.org:9200')
                .get(`/${aliasName}*/_settings/index.creation_date`)
                .reply(200, {
                    [aliasName + "_1"]:{ "settings": {"index": {"creation_date":now}}}
                });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);

            await estools.cleanupOldIndices(aliasName);

            expect(nock.isDone()).toBeTruthy();
        })

        it ('cleans up nothing because alias', async () => {

            const now = Date.now();

            //Set the prefix
            const aliasName = 'bryantestidx';

            //Setup the first nock
            const scope = nock('http://example.org:9200');

            //Setup the first nock for getIndicesOlderThan
            scope.get(`/${aliasName}*/_settings/index.creation_date`)
                .reply(200, {
                    [aliasName + "_1"]:{ "settings": {"index": {"creation_date": now }}}, //Should not be deleted
                    [aliasName + "_2"]:{ "settings": {"index": {"creation_date": moment(now).subtract(10, 'days').unix() }}} //Should not delete
                });

            // need nock for getIndicesForAlias
            scope.get(`/_alias/${aliasName}`)
                .reply(200, {
                    [aliasName + "_2"]: {
                        "aliases": {
                            [aliasName]: {}
                        }
                    }
                });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);
            await estools.cleanupOldIndices(aliasName);

            expect(nock.isDone()).toBeTruthy();
        })

        it ('cleans up indices', async () => {

            const now = Date.now();

            //Set the prefix
            const aliasName = 'bryantestidx';

            //Setup the first nock
            const scope = nock('http://example.org:9200');

            //Setup the first nock for getIndicesOlderThan
            scope.get(`/${aliasName}*/_settings/index.creation_date`)
                .reply(200, {
                    [aliasName + "_1"]:{ "settings": {"index": {"creation_date": now }}}, //Should not be deleted
                    [aliasName + "_2"]:{ "settings": {"index": {"creation_date": moment(now).subtract(10, 'days').unix() }}}, //Should delete
                    [aliasName + "_3"]:{ "settings": {"index": {"creation_date": moment(now).subtract(11, 'days').unix() }}} //Should delete
                });

            // need nock for getIndicesForAlias
            scope.get(`/_alias/${aliasName}`)
                .reply(404, {
                    "error": `alias [${aliasName}] missing`,
                    "status": 404
                });

            //Nock for delete.
            scope.delete(`/${aliasName}_2`)
                .reply(200, {
                    "acknowledged": true
                })
                .delete(`/${aliasName}_3`)
                .reply(200, {
                    "acknowledged": true
                });

            const client = new elasticsearch.Client({
                node: 'http://example.org:9200'
            });

            const estools = new ElasticTools(logger, client);
            await estools.cleanupOldIndices(aliasName);

            expect(nock.isDone()).toBeTruthy();
        });

    });

});
