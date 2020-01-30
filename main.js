let express = require('express');
let fs = require('fs');
var app = express();
let mysql= require('mysql');
app.listen(3000,() => console.log('port listening in port 3000'));

let dataT= fs.readFileSync('data/tickers.json')
let tickerData = JSON.parse(dataT);
let data= fs.readFileSync('data/data5000.json')
let fileData = JSON.parse(data);

let https= require('https');
var con = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "password",
  database:"stocklist8"
});

console.log(tickerData.length);

// Numbers of tickers already mined

// 839
// 300
// 450
// 400
// 400
// 224
// 253
// 353
// 500
// 400

// Data11=5000-5454

let a=5000;


let alphaVantageData=()=>{
  for (var i = 1, j = 1; i <= 453; i++, j++) {
    setTimeout(function() {
      console.log(tickerData[a]);    
      https.get('https://www.alphavantage.co/query?function=TIME_SERIES_WEEKLY_ADJUSTED&symbol='+tickerData[a]+'&apikey=****', (resp) => {
        let data = '';
        // A chunk of data has been recieved.
        resp.on('data', (chunk) => {
          data += chunk;
        });
        // The whole response has been received. Print out the result.
        resp.on('end', () => {
          let res = JSON.parse(data);
          console.log(res);
          fileData.push(res);
          let dataToSave=JSON.stringify(fileData);
          fs.writeFileSync('data/data5000.json', dataToSave)
          // fs.writeFileSync('data/dataBackUp.json', dataToSave)
        });
      });
      a++;
    }.bind(i), j * 13000);
  }
  console.log('ready')
}

let getQuarter=(date)=>{
  return Math.ceil(date.split('-')[1]/3);
}

function getType(len){
  let type=undefined;
  switch (len){
      case 1:
          type='Annual';
          break;
      case 2:
          type='Semi-Annual';
          break;
      case 4:
          type='Quarterly';
          break;
      case 12:
          type='Monthly';
          break;
      default:
        type = len;
        break;
  }
  return type;
}

async function getID(){
  return new Promise(resolve =>{
    con.query("SELECT ticker,id FROM tickers", function (err, result, fields) {
      if (err) throw err;
      let res={};
      for(var i=0;i<result.length;i++){
        res[result[i].ticker]=result[i].id;
      }
      resolve(res);
    });
  })
}

async function updateWeeklyData(relevant){
  return new Promise(resolve =>{
    class tickers{
      constructor(ticker,weeklyData){
          this.ticker = ticker;
          this.weeklyData = [weeklyData];
      }
    }

    class weekData{
      constructor(open,high,low,close,volume,dividend,quarter,year,month,day){
          this.open = open;
          this.high = high;
          this.low = low;
          this.close = close;
          this.volume = volume;
          this.dividend = dividend;
          this.quarter = quarter;
          this.year = year;
          this.month = month;
          this.day = day;
      }
    }

    async function saveW(){
      let count=0;
      let round=0;
      for(var a=1;a<=9;a++){
        let adata= fs.readFileSync('readyData/alpha/data'+a+'.json')
        let fileData = JSON.parse(adata);
        let weeklyData={};
        for(var i=0;i<fileData.length;i++){
          if(fileData[i]['Meta Data']){
            if(relevant.includes(fileData[i]['Meta Data']['2. Symbol'])){
              let ticker=fileData[i]['Meta Data']['2. Symbol'];
              weeklyData[ticker]=new tickers(ticker)
              weeklyData[ticker].weeklyData.pop();
              for(key in fileData[i]['Weekly Adjusted Time Series']){
                let week= new weekData(
                  fileData[i]['Weekly Adjusted Time Series'][key]['1. open'],
                  fileData[i]['Weekly Adjusted Time Series'][key]['2. high'],
                  fileData[i]['Weekly Adjusted Time Series'][key]['3. low'],
                  fileData[i]['Weekly Adjusted Time Series'][key]['4. close'],
                  fileData[i]['Weekly Adjusted Time Series'][key]['6. volume'],
                  fileData[i]['Weekly Adjusted Time Series'][key]['7. dividend amount'],
                  getQuarter(key),
                  key.split('-')[0],
                  key.split('-')[1],
                  key.split('-')[2],
                )
                weeklyData[ticker].weeklyData.push(week);
                count++;
              }
            }
          }
          if(count>20000){
            let ids = await getID();
            await updateWData(weeklyData,ids,round);
            weeklyData={};
            count=0;
            round++;
          }
        }
        let ids = await getID();
        if(Object.keys(weeklyData).length!==0){
          await updateWData(weeklyData,ids,round);          
        }
        weeklyData={};
        count=0;
        round++;
      }
      resolve('WeeklyData ready');     
    }
    saveW();
  })
}

async function updateWData(weeklyData,ids,round){
  return new Promise(resolve =>{
    let weekly=[];
    async function weekData(){
      for(key in weeklyData){
        for(var i=0;i<weeklyData[key].weeklyData.length;i++){
          weekly.push([
            weeklyData[key].weeklyData[i].open,
            weeklyData[key].weeklyData[i].high,
            weeklyData[key].weeklyData[i].low,
            weeklyData[key].weeklyData[i].close,
            weeklyData[key].weeklyData[i].volume,
            weeklyData[key].weeklyData[i].dividend,
            weeklyData[key].weeklyData[i].quarter,
            weeklyData[key].weeklyData[i].year,
            weeklyData[key].weeklyData[i].month,
            weeklyData[key].weeklyData[i].day,
            ids[key]
          ])
        }
      }
      console.log(await saveWeekly(weekly,round));
      resolve('Weekly data ready');      
    }
  weekData();
  })
}

let saveWeekly=(weekly,round)=>{
  return new Promise(resolve =>{
    let sql = "INSERT INTO weeklydata (open,high,low,close,volume,dividend,quarter,year,month,day,ticker_id) VALUES ?";
    con.query(sql, [weekly], function (err, result) {
        if (err) throw err;
        resolve("Round: "+round+" Number of weeklydata inserted: " + result.affectedRows);
    }); 
  });
}


let createTables=()=>{
  return new Promise(resolve =>{
    let sql = "DROP TABLE tickers,dividends,earnings,weeklydata";;
    con.query(sql, function (err, result) {
      if (err) throw err;
      console.log("All Tables deleted");
    });

    let sql1 = "CREATE TABLE tickers (id INT NOT NULL AUTO_INCREMENT,ticker VARCHAR(255) NOT NULL,name VARCHAR(255) NOT NULL,country VARCHAR(255) NOT NULL,dividendType VARCHAR(25),sector VARCHAR(255) NOT NULL,isin VARCHAR(255),PRIMARY KEY(id))"
    con.query(sql1, function (err, result) {
      if (err) throw err;
      console.log("Tickers Table created");
    });

    let sql2 = "CREATE TABLE dividends(id INT NOT NULL AUTO_INCREMENT,exDiv DATE NOT NULL,payDate DATE NOT NULL,dividend VARCHAR(255) NOT NULL,country VARCHAR(255) NOT NULL,year INT NOT NULL,month INT NOT NULL,day INT NOT NULL,quarter INT NOT NULL,ticker_id INT NOT NULL,PRIMARY KEY(id),FOREIGN KEY (ticker_id) REFERENCES tickers(id) )"
    con.query(sql2, function (err, result) {
      if (err) throw err;
      console.log("Dividends Table created");
    });

    let sql3 = "CREATE TABLE earnings(id INT NOT NULL AUTO_INCREMENT,eps VARCHAR(255),revenue VARCHAR(255),marketCap VARCHAR(255),date DATE NOT NULL,year INT NOT NULL,month INT NOT NULL,day INT NOT NULL,quarter INT NOT NULL,ticker_id INT NOT NULL,PRIMARY KEY(id),FOREIGN KEY (ticker_id) REFERENCES tickers(id) )"
    con.query(sql3, function (err, result) {
      if (err) throw err;
      console.log("Earnings Table created");
    });

    let sql4 = "CREATE TABLE weeklydata(id INT NOT NULL AUTO_INCREMENT,open VARCHAR(255),high VARCHAR(255),low VARCHAR(255),close VARCHAR(255),volume VARCHAR(255),dividend VARCHAR(255),quarter INT NOT NULL,    year INT NOT NULL,month INT NOT NULL,day INT NOT NULL,ticker_id INT NOT NULL,PRIMARY KEY(id),FOREIGN KEY (ticker_id) REFERENCES tickers(id))"
    con.query(sql4, function (err, result) {
      if (err) throw err;
      console.log("Weeklydata Table created");
      resolve('Create tables ready');
    });

  });
}

let combineData=(alphaData)=>{
  return new Promise(resolve =>{
    let dData= fs.readFileSync('readyData/invest/investingDividend.json')
    let investData = JSON.parse(dData);
    let list=[];
    for(key in alphaData){
      if(alphaData[key].type===0){
        list.push(key);
      }
    }
    let count=0;
    for(var i=0;i<list.length;i++){
      investData[list[i]].dividends.forEach(element => {
        let res = alphaData[list[i]].dividends.filter(div => (div.quarter==element.quarter && div.year==element.year)).length;
        if(res !==1){
          element.exDiv=element.exDiv.split('-')[0]+'.'+element.exDiv.split('-')[1]+'.'+element.exDiv.split('-')[2].split('T')[0];
          delete element.id;
          delete element.name;
          delete element.payDate;
          delete element.ticker_id;
          // delete element.exDiv;
          alphaData[list[i]].dividends.push(element);
        }   
      })
    }
    for(key in alphaData){
      alphaData[key].sector=investData[key].dividends[0].sector;
      alphaData[key].isin=investData[key].dividends[0].isin;
      alphaData[key].country=investData[key].dividends[0].country;
      alphaData[key].name=investData[key].dividends[0].name;
    }
    resolve(alphaData);
  })
}

let updateTickers=(cData)=>{
  return new Promise(resolve =>{
    // console.log(cData);
    let tickers=[];
    for(key in cData){
      tickers.push([
        key,
        cData[key].name===undefined ? 'undefined':cData[key].name,
        cData[key].country===undefined ? 'undefined':cData[key].country,
        cData[key].type===undefined ? 'undefined':cData[key].type,
        cData[key].sector===undefined ? 'undefined':cData[key].sector,
        cData[key].isin===undefined ? 'undefined':cData[key].isin,
      ])
    }
    // console.log(tickers);
    var sql = "INSERT INTO tickers (ticker, name, country, dividendType, sector, isin) VALUES ?";
    con.query(sql, [tickers], function (err, result) {
      if (err) throw err;
      resolve("Number of tickers inserted: " + result.affectedRows);
    });
  });
}

let updateEarnings=(ids,relevant)=>{
  return new Promise(resolve =>{
    let eaData= fs.readFileSync('readyData/earnings/fullEarningsData.json')
    let earningsData = JSON.parse(eaData);
    let earnings=[];
    for(key in earningsData){
      for(var i=0;i<earningsData[key].earnings.length;i++){
        if(relevant.includes(key)){
          if(ids[key]!==undefined){
            earnings.push([
              earningsData[key].earnings[i].eps,
              earningsData[key].earnings[i].revenue,
              earningsData[key].earnings[i].marketCap,
              earningsData[key].earnings[i].date.split('.')[1]+'.'+earningsData[key].earnings[i].date.split('.')[0]+'.'+earningsData[key].earnings[i].date.split('.')[2],
              earningsData[key].earnings[i].year,
              earningsData[key].earnings[i].month,
              earningsData[key].earnings[i].date.split('.')[1],
              earningsData[key].earnings[i].quarter,
              ids[key]
            ])
          }
        }
      }
    };
    var sql = "INSERT INTO earnings (eps,revenue,marketCap,date,year,month,day,quarter,ticker_id) VALUES ?";
    con.query(sql, [earnings], function (err, result) {
        if (err) throw err;
        console.log("Number of earnings inserted: " + result.affectedRows);
        resolve('Update earnings ready');
    });
  });
};

let createTickerData=(relevant)=>{
  console.log('Creating ticker data');
  return new Promise(resolve =>{
    async function saveDiv(){
      class divData{
        constructor(exDiv,dividend,quarter,year,month,day){
            this.exDiv = exDiv;
            this.dividend = dividend;
            this.quarter = quarter;
            this.year = year;
            this.month = month;
            this.day = day;
        }
      }

      class tickersDiv{
        constructor(ticker,type,dividends,weeklyData){
            this.ticker = ticker;
            this.type = type;
            this.dividends = [dividends];
            this.weeklyData=[weeklyData];
        }
      }

      for(var a=1;a<=9;a++){
        let adata= fs.readFileSync('readyData/alpha/data'+a+'.json')
        let fileData = JSON.parse(adata);

        let dividendData={};

        for(var i=0;i<fileData.length;i++){
          if(fileData[i]['Meta Data']){
            if(relevant.includes(fileData[i]['Meta Data']['2. Symbol'])){
              let ticker=fileData[i]['Meta Data']['2. Symbol'];
              dividendData[ticker]=new tickersDiv(ticker)
              dividendData[ticker].dividends.pop();
              for(key in fileData[i]['Weekly Adjusted Time Series']){
                if(fileData[i]['Weekly Adjusted Time Series'][key]['7. dividend amount']!=='0.0000'){
                  let dividend= new divData(
                    key.replace(/-/gi, "."),
                    fileData[i]['Weekly Adjusted Time Series'][key]['7. dividend amount'],
                    getQuarter(key),
                    key.split('-')[0],
                    key.split('-')[1],
                    key.split('-')[2],
                  )
                  dividendData[ticker].dividends.push(dividend);
                }
              }
          }
          }
        }
        if(Object.keys(dividendData).length!==0){
          for(key in dividendData){
              let len=dividendData[key].dividends.filter(dividend => dividend.year==='2019').length;
              let type = getType(len);
              dividendData[key].type=type;
          }
          let combinedData=await combineData(dividendData);
          console.log(await updateTickers(combinedData));
          let ids = await getID();
          console.log(await updateDividendData(combinedData,ids));          
        }
      }
      resolve('Update tickers ready');      
    }
    saveDiv();
  })
}

let saveDivData=(dividends)=>{
  return new Promise(resolve =>{  
    var sql = "INSERT INTO dividends (exDiv,payDate,dividend,country,year,month,day,quarter,ticker_id) VALUES ?";
    con.query(sql, [dividends], function (err, result) {
        if (err) throw err;
        resolve("Number of dividends inserted: " + result.affectedRows);
    });
  });
}

let updateDividendData=(DData,ids)=>{
  return new Promise(resolve =>{
    // let diData= fs.readFileSync('data/combinedData.json')
    // let DData = JSON.parse(diData);
    let dividends=[];
    async function updateDivData(){
      for(key in DData){
        for(var i=0;i<DData[key].dividends.length;i++){
            dividends.push([
              DData[key].dividends[i].exDiv,
              DData[key].dividends[i].exDiv,
              DData[key].dividends[i].dividend,
              DData[key].country,
              DData[key].dividends[i].year,
              DData[key].dividends[i].month,
              DData[key].dividends[i].day,
              DData[key].dividends[i].quarter,
              ids[key]
            ])
        }
        if(dividends.length>40000){
          console.log(await saveDivData(dividends))
          dividends=[];
        }
      }
      console.log(await saveDivData(dividends))
      resolve('Update dividends ready');      
    }


    updateDivData();
    // // Too many values to put once, use 2 patches
    // let firstH=dividends.splice(0, Math.ceil(dividends.length /2));
    // let secondH=dividends;


    // var sql = "INSERT INTO dividends (exDiv,payDate,dividend,country,year,month,day,quarter,ticker_id) VALUES ?";
    // con.query(sql, [secondH], function (err, result) {
    //     if (err) throw err;
    //     console.log("Number of dividends inserted: " + result.affectedRows);

    // });
  })
}

let checkRelevant=()=>{
  return new Promise(resolve =>{
  let eaData= fs.readFileSync('readyData/earnings/fullEarningsData.json')
  let earningsData = JSON.parse(eaData);
  let i=0;
  let relevant=[];
  for(key in earningsData){
    if(earningsData[key].earnings.length!=0){
      relevant.push(key);
      i++;
    }
  }
  resolve(relevant);
  })
}

alphaVantageData();

con.connect(function(err) {
  async function saveDataDB(){
    let relevant = await checkRelevant();
    console.log(await createTables());
    console.log(await createTickerData(relevant));
    let ids = await getID();
    console.log(await updateEarnings(ids,relevant));
    console.log(await updateWeeklyData(relevant));


    // console.log(await createDividendData(ids));
    // console.log(await saveWeekData(ids));
  }
  // saveDataDB();
});

















// var con = mysql.createConnection({
//     host: "localhost",
//     user: "root",
//     password: "password",
//     database:"stocklist5"
// });
  
// con.connect(function(err) {
//     class company{
//       constructor(ticker,dividends){
//         this.ticker=ticker,
//         this.dividends=[dividends]
//       }
//     }
//     if (err) throw err;
//     con.query("SELECT * FROM tickers JOIN dividends ON tickers.id =  dividends.ticker_id;", function (err, result, fields) {
//       if (err) throw err;
//       // console.log(result);
//       let tickerD={};
//       for(key in result){
//         // tickerD[result[key].ticker].push(result[key]);
//         // let di=result[key];
//         if(tickerD[result[key].ticker]){
//           tickerD[result[key].ticker].dividends.push(result[key]);
//         }else{
//           tickerD[result[key].ticker]=new company(result[key].ticker,result[key]);
//         }
//       }
//       console.log(tickerD['T']);
//       tickerD=JSON.stringify(tickerD);
//         fs.writeFileSync('data/investingDividend.json', tickerD)      
//     });
// });