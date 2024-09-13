#!/usr/bin/env node
const JSONStream = require('JSONStream');
const es = require('event-stream');
//const commandLineArgs = require('command-line-args');

// const optionDefinitions = [
//     { name: 'transform', alias: 't', type: String }
// ];
// const args = commandLineArgs(optionDefinitions);

process.stdin.setEncoding('utf8');

process.stdin
.pipe(JSONStream.parse())
.pipe(es.mapSync(function (obj) {
    let contracts = flatten(obj);
    // console.log(contracts);
    // process.exit(1);
    if(contracts.length > 0) {
        let output = '';
        contracts.map(c => output += JSON.stringify(c) + '\n');
        process.stdout.write(output);
    }
}))
// .pipe(process.stdout);

process.stdin.on('end', () => {
//   process.stdout.write('\n');
});

function flatten(obj) {
    let flatContracts = [];
    let release = obj.compiledRelease;

    if(release.tender.status == "complete") {
        if(release.awards && release.awards.length > 0) {
            release.awards.map( award => {
                if(award.status == "active") {
                    let contract = JSON.parse(JSON.stringify(release));
                    contract.parties = flattenParties(obj.compiledRelease.parties);
                    contract.awards = award;
                    contract.contracts = findContract(release, award);

                    flatContracts.push(contract)
                }
            } );
        }
    }

    return flatContracts;
}

function flattenParties(parties) {
    let flatParties = {}

    parties.map( party => {
        party.roles.map(r => {
            if(!flatParties.hasOwnProperty(r)) flatParties[r] = [];
            flatParties[r].push(party);
        });
    } );

    return flatParties;
}

function findContract(release, award) {
    let contract = null;
    if(release.hasOwnProperty('contracts') && release.contracts.length > 0) {
        release.contracts.map( c => {
            if(c.awardID == award.id) contract = c;
        } );
    }
    return contract;
}