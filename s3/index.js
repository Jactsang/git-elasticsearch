const s3 = require('aws-sdk/clients/s3');
const https = require('https');
const xlsx = require('xlsx');
const elasticsearch = require('../elasticsearch/index')

require('dotenv').config()

const options = {
    accessKeyId: process.env.AWS_ACCESS_KEY,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.S3_BUCKET_REGION,
    endpoint: process.env.S3_BUCKET_ENDPOINT,
};

const agentOptions = {
    rejectUnauthorized: false
};

const agent = new https.Agent(agentOptions);

(async () => {

    const client = new s3({
        ...options,
        s3ForcePathStyle: true,
        httpOptions: {
            agent
        }
    });

    client.listObjectsV2({
        Bucket: 'dsm-test'
    }, (err, bucketData) => {
        if (err) {
            console.error(err);
        } else {
            console.info(bucketData.Contents);
            console.log('total number of files: ', bucketData.Contents.length)
            if (bucketData.Contents.length != 0) {

                for(let i=0; i<1; i++){
                    ///// Start of getting data from an object ///// 
                client.getObject({
                    Bucket: 'dsm-test',
                    Key: bucketData.Contents[i].Key
                },
                    function (err, objectData) {
                        if (err) {
                            // send error when not getting data from an object
                            console.log(err, err.stack)
                        } else {
                            if(bucketData.Contents[i].Key.includes('.xlsx') || bucketData.Contents[i].Key.includes('.csv')){

                                const wb = xlsx.read(objectData.Body, {
                                    cellDates: true
                                })
                                console.log('name of the file: ', bucketData.Contents[i].Key)
                                console.log('number of spreadsheets: ',wb.SheetNames.length)
                                console.log('name of spreadsheets: ', wb.SheetNames)
                                
                                ///// start of reading data from excel file /////
                                if (wb.SheetNames.length != 0) {
                                    for (let w = 0; w < wb.SheetNames.length; w++) {
                                        const sheet = wb.Sheets[wb.SheetNames[w]]
                                        const sheet_data = xlsx.utils.sheet_to_json(sheet)
                                        console.log(`no. of items in sheet${w+1}`, sheet_data.length)
                                        const fileName = bucketData.Contents[i].Key.split("/").join("-").split(" ").join("-")
                                        const createdTime = bucketData.Contents[i].Key.split("/")[0]
                                        elasticsearch(fileName, wb.SheetNames[w], createdTime, bucketData.Contents[i].LastModified,sheet_data)
                                    }
                                }
                                ///// end of reading data from excel file /////
                        
                            }else{
                                console.log('there is some data here but it is not an excel file')
                            } 
                        }
                    })
             ///// End of getting data from an object /////   

                }
                
            }else{
                console.log('this bucket is empty')
            }
        }
    });

})();
