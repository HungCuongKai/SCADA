var AWS = require("aws-sdk");

AWS.config = {
  athena: '2017-05-18',
  region :"ap-southeast-1",
  accessKeyId : '',
  secretAccessKey : ""
  // other service API versions
};

var athena = new AWS.Athena();


// chờ lấy kết quả
function buildResult(id){
	return new Promise(
		function(resolve, reject){
			function poll(id) {
				console.log("polling..");
            athena.getQueryExecution({QueryExecutionId: id}, (err, data) => {
                if (err) return reject(err)
                if (data.QueryExecution.Status.State === 'SUCCEEDED') return resolve(getResult(id));
                else if (['FAILED', 'CANCELLED'].includes(data.QueryExecution.Status.State)) return reject(new Error(`Query ${data.QueryExecution.Status.State}`))
                else if( data.QueryExecution.Status.State == 'QUEUED') { setTimeout(poll, 2000, id) }
            })
        }
        poll(id);
		})
}

// lấy kết quả
function getResult(id){
	athena.getQueryResults({QueryExecutionId: id}, function(err, data) {
	  if (err) console.log(err, err.stack); // an error occurred
	  else  {
	  	var value = data.ResultSet.Rows; // cái này là kết quả 
	  }   console.log(value[2]);         // lấy record thứ 2 trong kết quả
	});
}

// thực hiện câu sql lưu vào s3
function executeQuery(){
	return new Promise(
		function (resolve, reject){
		var result = "";
		var params = {
		  QueryString: 'select * from  raw limit 10', /* required */
		 // ClientRequestToken: 'STRING_VALUE',
		  QueryExecutionContext: {
		    Catalog: 'AwsDataCatalog',
		    Database: 'demo'
		  },
		  ResultConfiguration: {
		    // EncryptionConfiguration: {
		    //   EncryptionOption: SSE_S3 | SSE_KMS | CSE_KMS, /* required */
		    // },
		    OutputLocation: 's3://iottesting/'
		  },
		};

		athena.startQueryExecution(params, function(err, data) {
		  if (err) reject(err);
		  else resolve(data.QueryExecutionId);   
		});
	})
}


function startSQL(sql){
	var pr1 = executeQuery()// sau nay sẽ truyền sql vào đây
	pr1.then((value) => {
	  	 buildResult(value);
	  }).catch((value) => {console.log(value)});
}


function test(id){
    athena.getQueryExecution({QueryExecutionId: id}, (err, data) => {
        if (err) console.log(err)
        else console.log(data.QueryExecution.Status.State);
    })
   
}

module.exports = {
    startSQL: startSQL,
    test : test
};