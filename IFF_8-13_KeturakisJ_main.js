const { start, dispatch, stop, spawnStateless, spawn } = require('nact');
const system = start();

const fs = require('fs');
const sha256 = require("crypto-js/sha256");

//Nurodomas duomenų failas.
const data = require('./IFF_8-13_KeturakisJ_dat_2.json');
//Nurodomas rezultatų failas.
const resultFile = './IFF_8-13_KeturakisJ_rez.txt';
const darbininkai = [];

console.log("Processing data...");

/**
 * Lyginami du objektai - pirmiau pagal metus
 * ir paskui pagal pažymį.
 * 
 * @param {Pirmas objektas} obj1 
 * @param {Antras objektas} obj2 
 */
function compareTo(obj1, obj2){
    if(obj1.year === obj2.year){
        return obj1.grade >= obj2.grade;
    }
    else{
        return obj1.year > obj2.year;
    }
}


/**
 * Kaupiklio aktorius - renka prafiltruotus duomenis
 * atsiųstus iš darbininkų per Skirstytuvo aktorių.
 */
const Kaupiklis = spawn(system, (state={}, msg, ctx) => {
    const hasFinished = msg.isDone !== undefined;
    const firstTime = state["firstTime"] === undefined;

    if(hasFinished){ // Surinkti rezultatai.
        const arr = state["array"];
        dispatch(Skirstytuvas, {sendToSpausdintojas: true, array: arr});
    }
    else if(firstTime){ // Kviečiamas pirmą kartą.
        const arr = [];
        arr.push(msg);
        return {...state, ["firstTime"]: true, ["array"]: arr};
    }
    else{ // Paima rezultatą ir padeda į atitinkamą poziciją.
        const arr = state["array"];
        arr.push(msg);
        arr.sort((obj1, obj2) => compareTo(obj1, obj2) ? 1 : -1);
        return state;
    }
});


/**
 * Spausdintuvo aktorius - gauna rezultatų masyvą iš Skirstytuvo
 * ir įrašo į tekstinį failą.
 */
const Spausdintuvas = spawnStateless(system, (msg, ctx) =>{
    //Surašomi pradiniai duomenys.
    console.log("Writing primary data...");

    const seperator = "-".repeat(105) + "\n";
    const primaryHeader = "Pradiniai duomenys:\n" + "#".padEnd(4) + " | " +"Name".padEnd(15) + " | " + "Year".padEnd(5) + " | " + "Grade".padEnd(5) + " | " + "Hash\n" + seperator;
    
    fs.unlink(resultFile, (err) => { });
    fs.appendFileSync(resultFile, primaryHeader);
    data.forEach((rez, i) => {        
        const year = rez.year.toString();
        const grade = rez.grade.toString();
        const line = (i + 1).toString().padEnd(4) + " | " + rez.name.padEnd(15) + " | " + year.padEnd(5) + " | " + grade.padEnd(5) + " | \n";
        fs.appendFileSync(resultFile, line);
    });

    //Surašomi rezultatai.
    console.log("Writing results...");

    const results = msg.array;
    const resultHeader = seperator + "Rezultatai:\n" + "#".padEnd(4) + " | " +"Name".padEnd(15) + " | " + "Year".padEnd(5) + " | " + "Grade".padEnd(5) + " | " + "Hash\n" + seperator;
    
    fs.appendFileSync(resultFile, resultHeader);
    results.forEach((rez, i) => {        
        const year = rez.year.toString();
        const grade = rez.grade.toString();
        const line = (i + 1).toString().padEnd(4) + " | " + rez.name.padEnd(15) + " | " + year.padEnd(5) + " | " + grade.padEnd(5) + " | " + rez.hash + '\n';
        fs.appendFileSync(resultFile, line);
    });

    fs.appendFileSync(resultFile, seperator);

    console.log("Finished writing results!");
});


/**
 * Skirstytuvo aktorius - skirsto žinutes tarp kitų aktorių.
 */
const Skirstytuvas = spawn(system, (state = {}, msg, ctx) => {
    const hasFinished = msg.isDone !== undefined;
    const fromWorker = msg.Kaupiklis !== undefined;
    const sendToSpausdintojas = msg.sendToSpausdintojas !== undefined;

    const dataCounter = state["dataCounter"] === undefined;
    const workers = state["workers"] === undefined;

    //Darbininkų skaitiklio inicializacija.
    if(workers){
        state = {...state, ["workers"]: 0};
    }

    //Duomenų skaitiklio inicializacija.
    if(dataCounter){
        state = {...state, ["dataCounter"]: 0};
    }

    /**
     * Siunčiama žinutė Kaupikliui, 
     * kad gražintų visą rezultatų masyvą,
     * kai visi darbininkai baigia darbą.
     */
    if(hasFinished){
        dispatch(Kaupiklis, msg);
    }
    else if(fromWorker){ // Žinutės iš darbininkų.
        
        //Rezultatas siunčiamas į Kaupiklį.
        if(msg.Kaupiklis){
            dispatch(Kaupiklis, msg)
        }

        //Sumažinamas darbininkų skaitiklis.
        const count = +state["workers"] - 1;
        state = {...state, ["workers"]: count};

        if(count == 0 && +state["dataCounter"] == data.length){
            //Baigia darbą paskutinis darbininkas.
            dispatch(Skirstytuvas, {isDone: true});
        }
    }
    else if(sendToSpausdintojas){
        dispatch(Spausdintuvas, msg);
    }
    else{
        const index = Math.round(Math.random()*1000) % darbininkai.length;
        dispatch(darbininkai[index], msg);

        const countWorking = 1 + +state["workers"];
        const countData = 1 + +state["dataCounter"];
        state = {...state, ["workers"]: countWorking, ["dataCounter"]: countData};
    }

    return state;
});


/**
 * Sukuriami darbininkų aktoriai.
 */
[...Array(data.length/4).keys()].forEach(elm => {
    darbininkai.push(spawnStateless(system, (msg, ctx) => {
        const tmp_arr = [msg.name];
        
        [...Array(1000).keys()].forEach(elm => {
            const hash = sha256(tmp_arr.pop() + msg.name + msg.year + msg.grade + msg.username);
            tmp_arr.push(hash);
        });

        const hash1 = tmp_arr.pop();

        if(msg.grade >= 7) {
            dispatch(Skirstytuvas, {Kaupiklis: true, hash: hash1, ...msg});
        }
        else{
            dispatch(Skirstytuvas, {Kaupiklis: false});
        }
    }));
});


//Siunčiami duomenys į Skirstytuvą.
data.map(x => dispatch(Skirstytuvas, x));