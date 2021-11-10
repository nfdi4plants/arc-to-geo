#!/usr/bin/env node
'use strict';

const FS = require('fs');
const PATH = require('path');
const SPAWN = require('child_process').spawn;

if(process.argv[2] !== '-p'){
  console.log('Usage: arc-to-geo -p PATH/TO/ARC');
  process.exit();
}

const intputPath = process.argv[3] || '';

const root = PATH.isAbsolute(intputPath) ? intputPath: PATH.join(process.cwd(),intputPath);

const enforceSet = (obj,name,value)=>{
  if(value===null || value===undefined)
    return;

  if(!obj.has(name))
    obj.set(name, new Set());
  obj.get(name).add(value);
};

const traverseDAG = (rdf,outputName,sequences,n)=>{
  for(let sequenceIdx=n; sequenceIdx>=0; sequenceIdx--){
    const sequence = sequences[sequenceIdx];
    const nOutputs = sequence.outputs.length;
    for(let outputIdx=0; outputIdx<nOutputs; outputIdx++){
      const output = sequence.outputs[outputIdx];
      if(output.name==outputName){
        const input = sequence.hasOwnProperty('inputs') ? sequence.inputs[outputIdx] : null;

        aggregateRawDataFileInformation(
          rdf,
          sequence,
          input,
          output
        );

        if(input){
          rdf.sampleName = input.name;
          traverseDAG(
            rdf,
            input.name,
            sequences,
            sequenceIdx-1
          );
        }
      }
    }
  }
};

const aggregateRawDataFileInformation = (rdf, sequence, input, output)=>{
  // add parameters
  for(let parameterValue of (sequence.parameterValues || [])){
    enforceSet(
      rdf.parameters,
      parameterValue.category.parameterName.annotationValue,
      parameterValue.value
    );
  }

  // add output factors
  if(output){
    for(let factorValue of (output.factorValues || [])){
      enforceSet(
        rdf.factors,
        factorValue.category.factorName,
        factorValue.value
      );
    }
  }

  if(input){
    // add input factors
    for(let factorValue of (input.factorValues || [])){
      enforceSet(
        rdf.factors,
        factorValue.category.factorName,
        factorValue.value
      );
    }

    // add input characteristics
    for(let characteristic of (input.characteristics || [])){
      enforceSet(
        rdf.characteristics,
        characteristic.category.characteristicType.annotationValue,
        characteristic.value
      );
    }
  }
};

function getRawDataFiles(assayJson){
  const rawDataFiles = new Map();

  // find all raw data files
  for(let sequence of assayJson.processSequence)
    for(let output of sequence.outputs)
      if(output.type === 'Raw Data File')
        rawDataFiles.set(output.name, {
          sampleName: null,
          name:output.name,
          characteristics: new Map(),
          parameters: new Map(),
          factors: new Map()
        });

  // augment raw data files with DAG information
  for(let [rdfName,rdf] of rawDataFiles)
    traverseDAG(
      rdf,
      rdfName,
      assayJson.processSequence,
      assayJson.processSequence.length-1
    );

  return rawDataFiles;
}

const toString = value => {
  return typeof value === 'object' ? value.annotationValue : value;
};

async function convert(arcJson){
  console.log(`Converting arc.json to geo.xlsx`);

  console.log(` - Annotating Header`);
  const EXCELJS = require('exceljs');
  const workbook = new EXCELJS.Workbook();
  await workbook.xlsx.readFile('./template.xlsx');

  const worksheet = workbook.getWorksheet(1);
  const findFirstOccurence = value=>{
    for(let i=1; i<=worksheet.rowCount; i++)
      if(worksheet.getRow(i).getCell(1).value === value)
        return i;
    return -1;
  };

  worksheet.getRow(findFirstOccurence('title')).getCell(2).value = arcJson.title;
  worksheet.getRow(findFirstOccurence('summary')).getCell(2).value = arcJson.description;

  // authors
  {
    let contributorRowIdx = findFirstOccurence('contributor');
    // delete key
    worksheet.spliceRows(contributorRowIdx, 1);
    // for each author add row
    for(let person of arcJson.people){
      worksheet.insertRows(contributorRowIdx++,[
        ['contributor', `${person.firstName},${person.lastName}`]
      ]);
    }
  }

  console.log(` - Aggregating Raw Data Files`);
  const rawDataFiles = getRawDataFiles(arcJson.studies[0].assays[0]);

  {
    console.log(` - Annotating Samples`);

    const rdfNames = [ ...rawDataFiles.keys() ];
    const firstRDF = rawDataFiles.get(rdfNames[0]);

    const pNames = [ ...firstRDF.parameters.keys() ].sort();
    const fNames = [ ...firstRDF.factors.keys() ].sort();
    const cNames = [ ...firstRDF.characteristics.keys() ].sort();

    let sampleRowIdx = findFirstOccurence('Sample name');
    const sampleHeaderRow = worksheet.getRow(sampleRowIdx);
    let offset = 2;
    for(let name of pNames)
      sampleHeaderRow.getCell(offset++).value = `[P]${name}`;
    for(let name of fNames)
      sampleHeaderRow.getCell(offset++).value = `[F]${name}`;
    for(let name of cNames)
      sampleHeaderRow.getCell(offset++).value = `[C]${name}`;
    sampleHeaderRow.getCell(offset++).value = `raw file`;

    for(let [rdfName,rdf] of rawDataFiles){
      sampleRowIdx++;
      const rowValues = [rdf.sampleName];

      for(
        let [p,names] of
        [
          ['parameters',pNames],
          ['factors',fNames],
          ['characteristics',cNames]
        ]
      ){
        for(let name of names)
          rowValues.push( [...rdf[p].get(name)].map(toString).join(';') );
      }
      rowValues.push( rdfName );

      worksheet.insertRows(sampleRowIdx,[
        rowValues
      ]);
    }
  }

  {
    console.log(` - Annotating Raw Files`);
    let rawFilesIdx = findFirstOccurence('RAW FILES')+2;

    for(let [rdfName,rdf] of rawDataFiles){
      worksheet.insertRows(
        rawFilesIdx++,
        [[rdfName]]
      );
    }
  }

  // prevent rows exceed bug
  worksheet.spliceRows(6500, 3000);

  await workbook.xlsx.writeFile('./geo.xlsx');

}

function getArcJson(){
  console.log(`Retrieving arc.json`);
  const arcProcess = SPAWN('arc', ['-v','0','export'] , {cwd:root});

  let jsonAsString = '';
  arcProcess.stdout.setEncoding('utf8');
  arcProcess.stdout.on('data', data=>jsonAsString+=data.toString());
  arcProcess.stderr.setEncoding('utf8');
  arcProcess.stderr.on('data', data=>console.error(data));
  arcProcess.on('close', code => convert(JSON.parse(jsonAsString)));
}

getArcJson();
