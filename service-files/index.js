const express = require('express');
const AWS = require('aws-sdk');
const RestaurantsMemcachedActions = require('./model/restaurantsMemcachedActions');

const app = express();
app.use(express.json());

const MEMCACHED_CONFIGURATION_ENDPOINT = process.env.MEMCACHED_CONFIGURATION_ENDPOINT;
const TABLE_NAME = process.env.TABLE_NAME;
const AWS_REGION = process.env.AWS_REGION;
const USE_CACHE = process.env.USE_CACHE === 'true';

const memcachedActions = new RestaurantsMemcachedActions(MEMCACHED_CONFIGURATION_ENDPOINT);

// Create a new DynamoDB instance
const dynamodb = new AWS.DynamoDB.DocumentClient({ region: AWS_REGION });

app.get('/', (req, res) => {
    const response = {
        MEMCACHED_CONFIGURATION_ENDPOINT: MEMCACHED_CONFIGURATION_ENDPOINT,
        TABLE_NAME: TABLE_NAME,
        AWS_REGION: AWS_REGION,
        USE_CACHE: USE_CACHE
    };
    res.send(response);
});

app.post('/restaurants', async (req, res) => {
    const restaurant = req.body;

    // Check if the restaurant already exists in the table before adding it (only dynamodb)
    const getParams = {
        TableName: TABLE_NAME,
        Key: {
            SimpleKey: restaurant.name
        }
    };

    try
    {
        const data = await dynamodb.get(getParams).promise();

        if (data.Item) {
            console.log("Restaurant already exists");
            res.status(409).send({ success: false, message: 'Restaurant already exists' });
            return;
        }

        console.log("Restaurant does not exist, adding it to the table");
    }

    catch (err)
    {
        console.error('Fatal error reading data from DynamoDB', err);
        res.status(500).send('Fatal error reading data from DynamoDB', err);
        return;
    }

    // Add the restaurant to the DynamoDB table
    const params = {
        TableName: TABLE_NAME,
        Item: {
            SimpleKey: restaurant.name,
            cuisine: restaurant.cuisine,
            region: restaurant.region,
            rating: 0
        }
    };

    await dynamodb.put(params).promise().then(() => {
        res.send({ success: true });
    }).catch(err => {
        console.error("Unable to add item. Error JSON:", JSON.stringify(err, null, 2));
        res.status(500).send("Unable to add item");
    });
});

app.get('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;

    // Check if the restaurant exists in the dynamodb table
    const params = {
        TableName: TABLE_NAME,
        Key: {
            SimpleKey: restaurantName
        }
    };

    try
    {
        const data = await dynamodb.get(params).promise();

        if (!data.Item) {
            console.log("Restaurant does not exist");
            res.status(404).send({ message: 'Restaurant not found' });
            return;
        }

        // Parse the restaurant data
        const restaurant = {
            name: data.Item.SimpleKey,
            cuisine: data.Item.cuisine,
            rating: data.Item.rating,
            region: data.Item.region
        };

        console.log("Restaurant found");
        res.status(200).send(restaurant);
    }

    catch (err)
    {
        console.error('Fatal error reading data from DynamoDB', err);
        res.status(500).send('Fatal error reading data from DynamoDB', err);
    }

    // Students TODO: Implement the logic to get a restaurant by name
    //res.status(404).send("need to implement");
});

app.delete('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;

    // Check if the restaurant exists in the dynamodb table (if not, return 404)
    const params = {
        TableName: TABLE_NAME,
        Key: {
            SimpleKey: restaurantName
        }
    };

    try
    {
        const data = await dynamodb.get(params).promise();

        if (!data.Item) {
            console.log('Restaurant', restaurantName, 'not found');
            res.status(404).send({ message: 'Restaurant not found' });
            return;
        }

        await dynamodb.delete(params).promise().then(() => {
            console.log('Restaurant', restaurantName, 'deleted successfully');
            res.send({ success: true });
        }).catch(err => {
            console.error('Unable to delete item', err);
            res.status(500).send('Unable to delete item', err);
        });
    }

    catch (err)
    {
        console.error('Fatal error reading data from DynamoDB', err);
        res.status(500).send('Fatal error reading data from DynamoDB', err);
        return;
    }
    
    // Students TODO: Implement the logic to delete a restaurant by name
    //res.status(404).send("need to implement");
});

app.post('/restaurants/rating', async (req, res) => {
    const restaurantName = req.body.name;
    const rating = req.body.rating;
    
    // Students TODO: Implement the logic to add a rating to a restaurant
    res.status(404).send("need to implement");
});

app.get('/restaurants/cuisine/:cuisine', async (req, res) => {
    const cuisine = req.params.cuisine;
    let limit = req.query.limit;
    
    // Students TODO: Implement the logic to get top rated restaurants by cuisine
    res.status(404).send("need to implement");
});

app.get('/restaurants/region/:region', async (req, res) => {
    const region = req.params.region;
    let limit = req.query.limit;
    
    // Students TODO: Implement the logic to get top rated restaurants by region
    res.status(404).send("need to implement");
});

app.get('/restaurants/region/:region/cuisine/:cuisine', async (req, res) => {
    const region = req.params.region;
    const cuisine = req.params.cuisine;

    // Students TODO: Implement the logic to get top rated restaurants by region and cuisine
    res.status(404).send("need to implement");
});

app.listen(80, () => {
    console.log('Server is running on http://localhost:80');
});

module.exports = { app };