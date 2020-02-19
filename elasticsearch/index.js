const elasticsearch = require('elasticsearch');
const config = require('config');
const esConfig = config.get('ELASTICSEARCH');

require('dotenv').config();

module.exports = async (fileName, sheetName, createdTime, lastModified, dataArray) => {

    const client = new elasticsearch.Client({
        // hosts: "http://localhost:9200"
        hosts: esConfig.HOSTS,
        httpAuth: process.env.ESAUTH,
        ssl: {
          ca: '../cert/ca.crt'
        },
        apiVersion: "6.8"
    })

    if (sheetName.includes('Sheet')) {
        sheetName = null
    }

    if (createdTime) {
        const year = createdTime.slice(0, 4)
        const month = createdTime.slice(4, 6)
        const day = createdTime.slice(6, 8)

        const newDate = new Date(year, month - 1, day)
        const newDateFormat = newDate.toLocaleDateString()
        createdTime = newDateFormat
    }

    if(lastModified){
        const newDateFormat = lastModified.toLocaleDateString()
        lastModified = newDateFormat
    }

    const body = dataArray.map((doc, index) => [
        { index: { _index: fileName.toLowerCase(), _type: 'test', _id: index+1 } },
        {
            title: sheetName,
            created: createdTime,
            lastModified: lastModified,
            doc
        }
    ])

    const flattenedBody = [].concat(...body)

    const response = await client.bulk({
        body: flattenedBody,
        refresh: true
    })

    if (response) {
        console.log(response.items)
        console.log(response.items[100])
        console.log('no. of input', response.items.length)
        console.log(response.items[0].index.error)
    }

}

const client = new elasticsearch.Client({
    // node: esConfig.HOSTS,
    // hosts: esConfig.HOSTS,
    hosts: "http://localhost:9200",
    // hosts: esConfig.HOSTS,
    // httpAuth: process.env.ESAUTH,
    // ssl: {
    //   ca: '../cert/ca.crt'
    // },
    // apiVersion: "6.8"
})

async function createBulkIndex() {
    const response = await client.bulk({
        body: flattenedBody,
        refresh: true
    })

    if (response) {
        console.log(response)
    }
}

// createBulkIndex().catch(console.log)

async function searchIndex() {
    const response = await client.search({
        index: '20200131-imc-3-esx-host.csv',
    });

    console.log(response.hits.hits.length)
}

// searchIndex().catch(console.log)

async function getIndex() {
    const response = await client.get({
        index: '20200131-imc-3-esx-host.csv',
        id: '110'
    })

    console.log(response);
}

// getIndex().catch(console.log);

async function removeIndex() {
    const response = await client.indices.delete({
        index: '20200131-imc-3-esx-host.csv',
    });

    console.log(response)
}

removeIndex().catch(console.log)






