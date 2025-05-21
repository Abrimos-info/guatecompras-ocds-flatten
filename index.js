#!/usr/bin/env node
const JSONStream = require('JSONStream');
const es = require('event-stream');
const commandLineArgs = require('command-line-args');

const optionDefinitions = [
    // - ocds:  flatten into OCDS-style release objects
    // - csv:   flatten into CSV-style JSON objects
    { name: 'output', alias: 'o', type: String }
];
const args = commandLineArgs(optionDefinitions);

process.stdin.setEncoding('utf8');

process.stdin
.pipe(JSONStream.parse())
.pipe(es.mapSync(function (obj) {
    let contracts = flatten(obj);

    if(contracts.length > 0) {
        let output = '';
        contracts.map(c => output += outputTransform(c, args.output) + '\n');
        process.stdout.write(output);
    }
}))
// .pipe(process.stdout);

process.stdin.on('end', () => {
//   process.stdout.write('\n');
});

function outputTransform(data, type='') {
    switch(type) {
        case 'csv':
            return JSON.stringify(csvTransform(data));
        default:
            return JSON.stringify(data);
    }
}

function csvTransform(data) {
    let meses = ['ENERO', 'FEBRERO', 'MARZO', 'ABRIL', 'MAYO', 'JUNIO', 'JULIO', 'AGOSTO', 'SEPTIEMBRE', 'OCTUBRE', 'NOVIEMBRE', 'DICIEMBRE'];
    let newObj = {
        fuente: 'ocds',
        tipo_entidad_padre: '',
        tipo_entidad: '',
        entidad_compradora: '',
        unidad_compradora: '',
        nog_concurso: '',
        descripcion: '',
        modalidad: '',
        sub_modalidad: 'N/A',
        nit: '',
        nombre: '',
        monto: 0,
        fecha_publicacion: '',
        mes_publicacion: '',
        anio_publicacion: '',
        fecha_ultima_adjudicacion: '',
        fecha_adjudicacion: '',
        mes_adjudicacion: '',
        anio_adjudicacion: '',
        categorias: [],
        estatus_concurso: '',
        fecha_cierre_recepcion_ofertas: '',
        mes_cierre_recepcion: '',
        anio_cierre_recepcion: '',
        dias_adjudicacion: 0,
        dias_oferta: 0
    }

    if(data.parties?.buyer) {
        data.parties.buyer.map( b => {
            if(b.memberOf?.length > 0) newObj.unidad_compradora = b.name;
            else {
                newObj.entidad_compradora = b.name;
                newObj.tipo_entidad = b.details.entityType.description;
                newObj.tipo_entidad_padre = b.details.type.description;
            }
        } )
    }
    newObj.nog_concurso = data.ocid.replace('ocds-xqjsxa-', '');
    newObj.descripcion = removeBreaks(data.awards.title);
    newObj.modalidad = data.tender.procurementMethodDetails;
    
    if(data.parties?.supplier) {
        newObj.nit = parseSupplierID(data.parties.supplier[0].id);
        newObj.nombre = data.parties.supplier[0].name;
    }
    
    newObj.monto = data.awards.value.amount;

    const fecha_publicacion = new Date(data.tender.datePublished);
    const fecha_adjudicacion = new Date(data.awards.date);
    const fecha_cierre_recepcion_ofertas = new Date(data.tender.tenderPeriod.endDate);

    newObj.fecha_publicacion = data.tender.datePublished;
    newObj.mes_publicacion = meses[fecha_publicacion.getMonth()];
    newObj.anio_publicacion = fecha_publicacion.getFullYear();
    
    newObj.fecha_ultima_adjudicacion = data.awards.date;
    newObj.fecha_adjudicacion = data.awards.date;
    newObj.mes_adjudicacion = meses[fecha_adjudicacion.getMonth()];
    newObj.anio_adjudicacion = fecha_adjudicacion.getFullYear();

    newObj.categorias = [getCategory(data.tender.mainProcurementCategory)];
    newObj.estatus_concurso = data.tender.statusDetails;

    newObj.fecha_cierre_recepcion_ofertas = data.tender.tenderPeriod.endDate;
    newObj.mes_cierre_recepcion = meses[fecha_cierre_recepcion_ofertas.getMonth()];
    newObj.anio_cierre_recepcion = fecha_cierre_recepcion_ofertas.getFullYear();

    newObj.dias_adjudicacion = Math.floor((fecha_adjudicacion.getTime() - fecha_cierre_recepcion_ofertas.getTime()) / (1000 * 3600 * 24)) - getDiasHabilesEntreFechas(fecha_cierre_recepcion_ofertas, fecha_adjudicacion);
    newObj.dias_oferta = Math.floor((fecha_cierre_recepcion_ofertas.getTime() - fecha_publicacion.getTime()) / (1000 * 3600 * 24)) - getDiasHabilesEntreFechas(fecha_publicacion, fecha_cierre_recepcion_ofertas);
    
    return newObj;
}

function flatten(obj) {
    let flatContracts = [];
    let release = obj;
    
    if(obj.hasOwnProperty('compiledRelease'))
        release = obj.compiledRelease;

    if(release?.tender?.status == "complete") {
        if(release.awards && release.awards.length > 0) {
            release.awards.map( award => {
                if(award.status == "active") {
                    let contract = JSON.parse(JSON.stringify(release));
                    contract.parties = flattenParties(release.parties);
                    contract.awards = award;
                    contract.contracts = findContract(release, award);

                    let suppliers = [];
                    if(contract.parties.supplier?.length > 0) {
                        contract.parties.supplier.map( s => {
                            award.suppliers.map( a => {
                                if(a.id == s.id) suppliers.push(s);
                            } )
                        } )
                        if(suppliers.length > 0)
                            contract.parties.supplier = suppliers;
                    }
                    else
                        contract.parties.supplier = award.suppliers;

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

function parseSupplierID(str) {
    let parts = str.split('-');
    return parts[parts.length - 1];
}

function getCategory(cat) {
    switch(cat) {
        case 'goods':
        case 'services':
            return 'Otros tipos de bienes o servicios';
        case 'works':
            return 'Construcción y materiales afines';
    }
}

function getDiasHabilesEntreFechas(fecha1, fecha2) {
    let diaMillis = 1 * 1000 * 60 * 60 * 24;
    let dias_habiles = 0;

    while(fecha1 < fecha2) {
        //Si es sábado o domingo o feriado
        if (fecha1.getDay() == 6 || fecha1.getDay() == 0 || esFeriado(fecha1)) {
            dias_habiles++
        }
        fecha1 = new Date(fecha1.setTime(fecha1.getTime() + diaMillis));
    }
    return dias_habiles;
}

const feriados = [
    { anio: 2003, mes: 0, dia: 1 },
    { anio: 2003, mes: 1, dia: 5 },
    { anio: 2003, mes: 2, dia: 21 },
    { anio: 2003, mes: 4, dia: 1 },
    { anio: 2003, mes: 8, dia: 16 },
    { anio: 2003, mes: 10, dia: 20 },
    { anio: 2003, mes: 11, dia: 25 },
    { anio: 2004, mes: 0, dia: 1 },
    { anio: 2004, mes: 1, dia: 5 },
    { anio: 2004, mes: 2, dia: 21 },
    { anio: 2004, mes: 4, dia: 1 },
    { anio: 2004, mes: 8, dia: 16 },
    { anio: 2004, mes: 10, dia: 20 },
    { anio: 2004, mes: 11, dia: 25 },
    { anio: 2005, mes: 0, dia: 1 },
    { anio: 2005, mes: 1, dia: 5 },
    { anio: 2005, mes: 2, dia: 21 },
    { anio: 2005, mes: 4, dia: 1 },
    { anio: 2005, mes: 8, dia: 16 },
    { anio: 2005, mes: 10, dia: 20 },
    { anio: 2005, mes: 11, dia: 25 },
    { anio: 2006, mes: 0, dia: 1 },
    { anio: 2006, mes: 1, dia: 5 },
    { anio: 2006, mes: 2, dia: 21 },
    { anio: 2006, mes: 4, dia: 1 },
    { anio: 2006, mes: 6, dia: 2 },
    { anio: 2006, mes: 8, dia: 16 },
    { anio: 2006, mes: 10, dia: 20 },
    { anio: 2006, mes: 11, dia: 1 },
    { anio: 2006, mes: 11, dia: 25 },
    { anio: 2007, mes: 0, dia: 1 },
    { anio: 2007, mes: 1, dia: 5 },
    { anio: 2007, mes: 2, dia: 19 },
    { anio: 2007, mes: 4, dia: 1 },
    { anio: 2007, mes: 8, dia: 16 },
    { anio: 2007, mes: 10, dia: 19 },
    { anio: 2007, mes: 10, dia: 20 },
    { anio: 2007, mes: 11, dia: 25 },
    { anio: 2008, mes: 0, dia: 1 },
    { anio: 2008, mes: 1, dia: 4 },
    { anio: 2008, mes: 2, dia: 17 },
    { anio: 2008, mes: 4, dia: 1 },
    { anio: 2008, mes: 8, dia: 16 },
    { anio: 2008, mes: 10, dia: 17 },
    { anio: 2008, mes: 10, dia: 20 },
    { anio: 2008, mes: 11, dia: 25 },
    { anio: 2009, mes: 0, dia: 1 },
    { anio: 2009, mes: 1, dia: 2 },
    { anio: 2009, mes: 2, dia: 16 },
    { anio: 2009, mes: 4, dia: 1 },
    { anio: 2009, mes: 8, dia: 16 },
    { anio: 2009, mes: 10, dia: 16 },
    { anio: 2009, mes: 10, dia: 20 },
    { anio: 2009, mes: 11, dia: 25 },
    { anio: 2010, mes: 0, dia: 1 },
    { anio: 2010, mes: 1, dia: 1 },
    { anio: 2010, mes: 2, dia: 15 },
    { anio: 2010, mes: 4, dia: 1 },
    { anio: 2010, mes: 8, dia: 16 },
    { anio: 2010, mes: 10, dia: 20 },
    { anio: 2010, mes: 10, dia: 22 },
    { anio: 2010, mes: 11, dia: 25 },
    { anio: 2011, mes: 0, dia: 1 },
    { anio: 2011, mes: 1, dia: 7 },
    { anio: 2011, mes: 2, dia: 21 },
    { anio: 2011, mes: 4, dia: 1 },
    { anio: 2011, mes: 8, dia: 16 },
    { anio: 2011, mes: 11, dia: 25 },
    { anio: 2012, mes: 0, dia: 1 },
    { anio: 2012, mes: 1, dia: 6 },
    { anio: 2012, mes: 2, dia: 19 },
    { anio: 2012, mes: 4, dia: 1 },
    { anio: 2012, mes: 6, dia: 1 },
    { anio: 2012, mes: 8, dia: 16 },
    { anio: 2012, mes: 11, dia: 1 },
    { anio: 2012, mes: 11, dia: 25 },
    { anio: 2013, mes: 0, dia: 1 },
    { anio: 2013, mes: 1, dia: 4 },
    { anio: 2013, mes: 2, dia: 18 },
    { anio: 2013, mes: 4, dia: 1 },
    { anio: 2013, mes: 8, dia: 16 },
    { anio: 2013, mes: 11, dia: 25 },
    { anio: 2014, mes: 0, dia: 1 },
    { anio: 2014, mes: 1, dia: 3 },
    { anio: 2014, mes: 2, dia: 17 },
    { anio: 2014, mes: 4, dia: 1 },
    { anio: 2014, mes: 8, dia: 16 },
    { anio: 2014, mes: 11, dia: 25 },
    { anio: 2015, mes: 0, dia: 1 },
    { anio: 2015, mes: 1, dia: 2 },
    { anio: 2015, mes: 2, dia: 16 },
    { anio: 2015, mes: 4, dia: 1 },
    { anio: 2015, mes: 8, dia: 16 },
    { anio: 2015, mes: 11, dia: 25 },
    { anio: 2016, mes: 0, dia: 1 },
    { anio: 2016, mes: 1, dia: 1 },
    { anio: 2016, mes: 2, dia: 21 },
    { anio: 2016, mes: 4, dia: 1 },
    { anio: 2016, mes: 8, dia: 16 },
    { anio: 2016, mes: 11, dia: 25 },
    { anio: 2017, mes: 0, dia: 1 },
    { anio: 2017, mes: 1, dia: 6 },
    { anio: 2017, mes: 2, dia: 20 },
    { anio: 2017, mes: 4, dia: 1 },
    { anio: 2017, mes: 8, dia: 16 },
    { anio: 2017, mes: 11, dia: 25 },
    { anio: 2018, mes: 0, dia: 1 },
    { anio: 2018, mes: 1, dia: 5 },
    { anio: 2018, mes: 2, dia: 19 },
    { anio: 2018, mes: 4, dia: 1 },
    { anio: 2018, mes: 6, dia: 1 },
    { anio: 2018, mes: 8, dia: 16 },
    { anio: 2018, mes: 11, dia: 1 },
    { anio: 2018, mes: 11, dia: 25 },
    { anio: 2019, mes: 0, dia: 1 },
    { anio: 2019, mes: 1, dia: 4 },
    { anio: 2019, mes: 2, dia: 18 },
    { anio: 2019, mes: 4, dia: 1 },
    { anio: 2019, mes: 8, dia: 16 },
    { anio: 2019, mes: 11, dia: 25 },
    { anio: 2020, mes: 0, dia: 1 },
    { anio: 2020, mes: 1, dia: 3 },
    { anio: 2020, mes: 2, dia: 16 },
    { anio: 2020, mes: 4, dia: 1 },
    { anio: 2020, mes: 8, dia: 16 },
    { anio: 2020, mes: 11, dia: 25 },
    { anio: 2021, mes: 0, dia: 1 },
    { anio: 2021, mes: 1, dia: 1 },
    { anio: 2021, mes: 2, dia: 15 },
    { anio: 2021, mes: 4, dia: 1 },
    { anio: 2021, mes: 8, dia: 16 },
    { anio: 2021, mes: 11, dia: 25 },
    { anio: 2022, mes: 0, dia: 1 },
    { anio: 2022, mes: 1, dia: 7 },
    { anio: 2022, mes: 2, dia: 21 },
    { anio: 2022, mes: 4, dia: 1 },
    { anio: 2022, mes: 8, dia: 16 },
    { anio: 2022, mes: 11, dia: 25 },
    { anio: 2023, mes: 0, dia: 1 },
    { anio: 2023, mes: 1, dia: 6 },
    { anio: 2023, mes: 2, dia: 20 },
    { anio: 2023, mes: 4, dia: 1 },
    { anio: 2023, mes: 8, dia: 16 },
    { anio: 2023, mes: 10, dia: 20 },
    { anio: 2023, mes: 11, dia: 25 }
];

function esFeriado(dia) {

    for (let f in feriados) {
        if (dia.getFullYear() == feriados[f].anio && dia.getMonth() == feriados[f].mes && dia.getDate() == feriados[f].dia) {
            return true;
        }
    }
    return false;
}

function removeBreaks(str) {
    return str.replace(/\<br\s+\/\>/g, ' ').trim();
}
