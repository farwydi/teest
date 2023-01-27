const mongodb = require('mongodb');

const uri =
    process.env.MONGO_DB || '';

exports.main = async (args) => {
    if (uri === '') {
        return {
            error: "empty mongo uri"
        }
    }

    const client = new mongodb.MongoClient(uri, {monitorCommands: true});
    client.on('commandStarted', (event) => console.debug(event));
    client.on('commandSucceeded', (event) => console.debug(event));
    client.on('commandFailed', (event) => console.debug(event));

    const connect = await client.connect()

    console.log("Connected successfully");

    try {
        const database = connect.db('admin');
        const gears = database.collection('gears');

        return await gears.aggregate([
            {
                $match: {className: "WARLOCK", slot: "TRINKET"},
            },
            {
                $sort: {id: -1},
            },
            {
                $group: {
                    _id: {
                        className: "$className",
                        slot: "$slot",
                        ranking: "$ranking",
                    },
                    combine: {$push: "$id"},
                    items: {
                        $push: {
                            id: "$id",
                            itemLevel: "$itemLevel",
                        },
                    },
                },
            },
            {
                $group: {
                    _id: {
                        className: "$_id.className",
                        slot: "$_id.slot",
                        combine: "$combine",
                    },
                    items: {
                        $push: "$items",
                    },
                    count: {$count: {}},
                },
            },
            {
                $sort: {count: -1}
            },
            {
                $project: {
                    _id: 0,
                    className: "$_id.className",
                    slot: "$_id.slot",
                    items: {
                        $reduce: {
                            input: "$items",
                            initialValue: {$first: "$items"},
                            in: {
                                // $$this   - [{itemId: 1}, {itemId: 2}]
                                // $$value  - accumulator
                                $map: {
                                    input: {$range: [0, {$size: "$$value"}]},
                                    as: "itemIndex",
                                    in: {
                                        $let: {
                                            vars: {
                                                thisItem: {$arrayElemAt: ["$$this", "$$itemIndex"]},
                                                accItem: {$arrayElemAt: ["$$value", "$$itemIndex"]},
                                            },
                                            in: {
                                                id: "$$accItem.id",
                                                maxItemLevel: {$max: ["$$accItem.maxItemLevel", "$$thisItem.itemLevel"]},
                                                minItemLevel: {$min: ["$$accItem.minItemLevel", "$$thisItem.itemLevel"]},
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                    count: 1,
                },
            },
            {
                $limit: 5,
            }
        ]).toArray()
    } finally {
        console.log("done")

        // Ensures that the client will close when you finish/error
        await client.close();
    }
}

if (process.env.TEST) exports.main({}).then(console.log)
