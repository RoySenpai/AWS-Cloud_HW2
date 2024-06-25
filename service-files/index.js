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

    if (!restaurant.name || !restaurant.cuisine || !restaurant.region) {
        console.error('POST /restaurants', 'Missing required fields');
        res.status(400).send({ success: false, message: 'Missing required fields' });
        return;
    }

    if (USE_CACHE) {
        console.error('POST /restaurants', 'need to implement cache');
        res.status(404).send("need to implement cache");
        return;
    }

    // Check if the restaurant already exists in the table before adding it (only dynamodb)
    const getParams = {
        TableName: TABLE_NAME,
        Key: {
            SimpleKey: restaurant.name
        }
    };

    try {
        const data = await dynamodb.get(getParams).promise();

        if (data.Item) {
            res.status(409).send({ success: false, message: 'Restaurant already exists' });
            return;
        }

    } catch (err) {
        console.error('POST /restaurants', err);
        res.status(500).send("Internal Server Error");
        return;
    }

    // Add the restaurant to the DynamoDB table
    const params = {
        TableName: TABLE_NAME,
        Item: {
            SimpleKey: restaurant.name,
            Cuisine: restaurant.cuisine,
            GeoRegion: restaurant.region,
            Rating: 0,
            RatingCount: 0
        }
    };

    try {
        await dynamodb.put(params).promise();
        res.status(200).send({ success: true });
    } catch (err) {
        console.error('POST /restaurants', err);
        res.status(500).send("Internal Server Error");
    }
});

app.get('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;

    if (USE_CACHE) {
        console.error('GET /restaurants/:restaurantName', 'need to implement cache');
        res.status(404).send("need to implement cache");
        return;
    }

    // Check if the restaurant exists in the dynamodb table
    const params = {
        TableName: TABLE_NAME,
        Key: {
            SimpleKey: restaurantName
        }
    };

    try {
        const data = await dynamodb.get(params).promise();

        if (!data.Item) {
            res.status(404).send({ message: 'Restaurant not found' });
            return;
        }

        // Parse the restaurant data
        const restaurant = {
            name: data.Item.SimpleKey,
            cuisine: data.Item.Cuisine,
            rating: data.Item.Rating,
            region: data.Item.GeoRegion
        };

        res.status(200).send(restaurant);
    } catch (err) {
        console.error('GET /restaurants/:restaurantName', err);
        res.status(500).send('Internal Server Error');
    }
});

app.delete('/restaurants/:restaurantName', async (req, res) => {
    const restaurantName = req.params.restaurantName;

    if (USE_CACHE) {
        res.status(404).send("need to implement cache");
        return;
    }

    // Check if the restaurant exists in the dynamodb table (if not, return 404)
    const params = {
        TableName: TABLE_NAME,
        Key: {
            SimpleKey: restaurantName
        }
    };

    try {
        const data = await dynamodb.get(params).promise();

        if (!data.Item) {
            res.status(404).send({ message: 'Restaurant not found' });
            return;
        }

        await dynamodb.delete(params).promise();
        console.log('Restaurant', restaurantName, 'deleted successfully');
        res.status(200).send({ success: true });
    } catch (err) {
        console.error('DELETE /restaurants/:restaurantName', err);
        res.status(500).send('Internal Server Error');
    }
});

app.post('/restaurants/rating', async (req, res) => {
    const restaurantName = req.body.name;
    const newRating = req.body.rating;

    if (!restaurantName || !newRating) {
        console.error('POST /restaurants/rating', 'Missing required fields');
        res.status(400).send({ success: false, message: 'Missing required fields' });
        return;
    }

    // Get the current data for the restaurant
    const params = {
        TableName: TABLE_NAME,
        Key: {
            SimpleKey: restaurantName
        }
    };

    try {
        const data = await dynamodb.get(params).promise();

        if (!data.Item) {
            res.status(404).send("Restaurant not found");
            return;
        }

        // Calculate the new average rating
        const oldRating = data.Item.Rating || 0;
        const ratingCount = data.Item.RatingCount || 0;
        const newAverageRating = ((oldRating * ratingCount) + newRating) / (ratingCount + 1);

        // Update the restaurant's rating
        const updateParams = {
            TableName: TABLE_NAME,
            Key: {
                SimpleKey: restaurantName
            },
            UpdateExpression: 'set Rating = :r, RatingCount = :rc',
            ExpressionAttributeValues: {
                ':r': newAverageRating,
                ':rc': ratingCount + 1
            }
        };

        await dynamodb.update(updateParams).promise();

        res.status(200).send({ success: true });
    } catch (error) {
        console.error('POST /restaurants/rating', error);
        res.status(500).send("Internal Server Error");
    }
});

app.get('/restaurants/cuisine/:cuisine', async (req, res) => {
    const cuisine = req.params.cuisine;
    let limit = parseInt(req.query.limit) || 10;
    limit = Math.min(limit, 100);
    const minRating = parseFloat(req.query.minRating) || 0;

    if (!cuisine) {
        console.error('GET /restaurants/cuisine/:cuisine', 'Missing required fields');
        res.status(400).send( { success: false, message: 'Missing required fields' });
        return;
    }

    if (minRating < 0 || minRating > 5) {
        console.error('GET /restaurants/cuisine/:cuisine', 'Invalid rating');
        res.status(400).send( { success: false, message: 'Invalid rating' });
        return;
    }

    if (USE_CACHE) {
        console.error('GET /restaurants/cuisine/:cuisine', 'need to implement cache');
        res.status(404).send("need to implement cache");
        return;
    }

    const params = {
        TableName: TABLE_NAME,
        IndexName: 'CuisineIndex',
        KeyConditionExpression: 'Cuisine = :cuisine',
        FilterExpression: 'Rating >= :minRating',
        ExpressionAttributeValues: {
            ':cuisine': cuisine,
            ':minRating': minRating
        },
        Limit: limit,
        ScanIndexForward: false // to get top-rated restaurants
    };

    try {
        const data = await dynamodb.query(params).promise();

        const restaurants = data.Items.map(item => {
            return {
                cuisine: item.Cuisine,
                name: item.SimpleKey,
                rating: item.Rating,
                region: item.GeoRegion
            };
        });

        res.json(restaurants);
    } catch (error) {
        console.error('GET /restaurants/cuisine/:cuisine', error);
        res.status(500).send("Internal Server Error");
    }
});

app.get('/restaurants/region/:region', async (req, res) => {
    const region = req.params.region;
    let limit = parseInt(req.query.limit) || 10;
    limit = Math.min(limit, 100);

    if (!region) {
        console.error('GET /restaurants/region/:region', 'Missing required fields');
        res.status(400).send( { success: false, message: 'Missing required fields' });
        return;
    }

    if (USE_CACHE) {
        console.error('GET /restaurants/region/:region', 'need to implement cache');
        res.status(404).send("need to implement cache");
        return;
    }

    const params = {
        TableName: TABLE_NAME,
        IndexName: 'GeoRegionIndex',
        KeyConditionExpression: 'GeoRegion = :geoRegion',
        ExpressionAttributeValues: {
            ':geoRegion': region
        },
        Limit: limit,
        ScanIndexForward: false // to get top-rated restaurants
    };

    try {
        const data = await dynamodb.query(params).promise();

        const restaurants = data.Items.map(item => {
            return {
                cuisine: item.Cuisine,
                name: item.SimpleKey,
                rating: item.Rating,
                region: item.GeoRegion
            };
        });

        res.json(restaurants);
    } catch (error) {
        console.error('GET /restaurants/region/:region', error);
        res.status(500).send("Internal Server Error");
    }
});

app.get('/restaurants/region/:region/cuisine/:cuisine', async (req, res) => {
    const region = req.params.region;
    const cuisine = req.params.cuisine;
    let limit = parseInt(req.query.limit) || 10;
    limit = Math.min(limit, 100);

    if (!region || !cuisine) {
        console.error('GET /restaurants/region/:region/cuisine/:cuisine', 'Missing required fields');
        res.status(400).send( { success: false, message: 'Missing required fields' });
        return;
    }

    if (USE_CACHE) {
        console.error('GET /restaurants/region/:region/cuisine/:cuisine', 'need to implement cache');
        res.status(404).send("need to implement cache");
        return;
    }

    const params = {
        TableName: TABLE_NAME,
        IndexName: 'GeoRegionCuisineIndex',
        KeyConditionExpression: 'GeoRegion = :geoRegion and Cuisine = :cuisine',
        ExpressionAttributeValues: {
            ':geoRegion': region,
            ':cuisine': cuisine
        },
        Limit: limit,
        ScanIndexForward: false // to get top-rated restaurants
    };

    try {
        const data = await dynamodb.query(params).promise();
        
        const restaurants = data.Items.map(item => {
            return {
                cuisine: item.Cuisine,
                name: item.SimpleKey,
                rating: item.Rating,
                region: item.GeoRegion
            };
        });

        res.json(restaurants);
    } catch (error) {
        console.error('GET /restaurants/region/:region/cuisine/:cuisine', error);
        res.status(500).send('Internal Server Error');
    }
});

app.listen(80, () => {
    console.log('Server is running on http://localhost:80');
});

module.exports = { app };