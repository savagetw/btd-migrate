'use strict';

var db = require('odbc')();
var transformAddresses = require('./transformers/addresses.js');
var experiencesTransformer = require('./transformers/experiences.js');
var jobTransformer = require('./transformers/jobs.js');

var dbFile = process.argv[2];
if (!dbFile) {
    console.log('Specify accdb file in this directory.');
    process.exit(1);
}

var cn = "DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; Dbq=.\\" + dbFile;
var Promise = require('bluebird');
var _ = require('lodash');
var fs = require('fs');
var Random = require('meteor-random');

Promise.promisifyAll(db);

console.log('Provider string:', cn);

var people = [];
var PeopleByMigrationId = {};
var WeekendRolesByMigrationId = {};
db.openAsync(cn)
    .then(function() {  
    	console.log('Connected!');
        return processTables(db, [
            {name: 'Addresses', collection: 'people', transform: transformAddresses},
            {name: 'Archived Addresses', collection: 'people', transform: transformInactiveAddresses}, 
        ]);
    })
    .spread(function (activeAddressesTransformations, inactiveAddressesTransformations) {
        // Combine into a single all people collection
        let activePeople = _.find(activeAddressesTransformations, {name: 'People'}).collection;
        let inactivePeople = _.find(inactiveAddressesTransformations, {name: 'People'}).collection;
        people = activePeople.concat(inactivePeople);

        // Apparently, to workaround the application constraints of people working on an
        // alternative gender weekend (e.g. Skip Massey working Transportation on a Female
        // weekend), duplicate records were created for such a person... just with an
        // alternative gender. :-/
        people = dedupePeople(people);

        // Get a hash to improve lookup time for other operations
        PeopleByMigrationId = buildHash(people);

        // For each person, resolve their sponsor's details
        resolveSponsors(people, PeopleByMigrationId);

        return processTables(db, [
            {name: 'Table Names', collection: 'tables', transform: transformTables},
            {name: 'Team Jobs', collection: 'weekend-roles', transform: jobTransformer.transform}, 
            {name: 'ExperienceMale', collection: 'weekends-male', transform: transformMaleExperience}, 
            {name: 'ExperienceFemale', collection: 'weekends-female', transform: transformFemaleExperience}
        ]);
    })
    .spread(function (tableNames, teamJobsTransformations, maleExperiencesTransformations, femaleExperiencesTransformations) {
        var maleWeekends = _.find(maleExperiencesTransformations, {name: 'Weekends'}).collection;
        var femaleWeekends = _.find(femaleExperiencesTransformations, {name: 'Weekends'}).collection;
        var weekends = maleWeekends.concat(femaleWeekends);

        var weekendRoles = _.find(teamJobsTransformations, {name: 'WeekendRoles'}).collection;
        WeekendRolesByMigrationId = buildHash(weekendRoles);

        for (let migrationId in PeopleByMigrationId) {
            addPeopleToCandidateWeekends(PeopleByMigrationId[migrationId], weekends);
        }

        weekends.forEach(function (weekend) {
            resolvePeopleAndRoles(weekend, PeopleByMigrationId, WeekendRolesByMigrationId);
        });

        writeFile('people.json', JSON.stringify(people));
        writeFile('weekends.json', JSON.stringify(weekends));
        writeFile('weekendRoles.json', JSON.stringify(weekendRoles));
    })
    .then(function () {
        return db.closeAsync();
    })
    .catch(function(err) {
        console.log('Caught err: ', err);
        throw err;
    });

function dedupePeople(people) {
    var peopleByName = {};

    return people.filter(function isUnique(person) {
        let key = getKey(person);
        var found = peopleByName[key];
        if (found) {
            mergeDuplicate(found, person);
            found.duplicates = found.duplicates || [];
            found.duplicates.push(person.migrationId);
            return false;
        }

        peopleByName[key] = person;
        return true;
    });

    function getKey(person) {
        return person.firstName + '.' + person.lastName;
    }

    function mergeDuplicate(person, duplicate) {
        let merged = {};
        if (person.status === 'active' && person.address.city) {
            _.defaultsDeep(merged, person, duplicate);
        } else {
            _.defaultsDeep(merged, duplicate, person);
        }
        _.assign(person, merged);
    }
}

function buildHash(collection) {
    let result = {};
    collection.forEach(function (thing) {
        result[thing.migrationId] = thing;
        delete thing.migrationId;

        if (thing.duplicates) {
            thing.duplicates.forEach(function (duplicateMigrationId) {
                result[duplicateMigrationId] = thing;
            });
            delete thing.duplicates;
        }
    });
    return result;
}

function resolveSponsors(people, peopleByMigrationId) {
    people.forEach(function (person) {
        person.migrationSponsorSearch.some(function (migrationSponsorId) {
            var sponsor = peopleByMigrationId[migrationSponsorId];
            if (sponsor) {
                person.sponsor = {
                    firstName: sponsor.firstName,
                    lastName: sponsor.lastName,
                    preferredName: sponsor.preferredName,
                    _id: sponsor._id
                };
                return true;
            }
            return false;
        });
        delete person.migrationSponsorSearch;
    });
}

function writeFile(filename, data) {
    var dir = 'output';
    if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir);
    }
    fs.writeFileSync(dir + '/' + filename, data);
    console.log('Wrote ' + filename);
}

function processTables(db, tables) {
    return Promise.all(tables.map(function (table) {
        return db.queryAsync('SELECT * FROM `' + table.name + '`')
            .then(function (data) {
                return table.transform(data);
            });
    }));
}

function resolvePeopleAndRoles(weekend, PeopleByMigrationId, WeekendRolesByMigrationId) {
    weekend.attendees = weekend.attendees.filter(function (attendee) {
        if (!attendee.migrationPersonId) {
            return true;
        }

        var person = PeopleByMigrationId[attendee.migrationPersonId];
        if (!person) {
            console.log('WARN: Cannot resolve person for Weekend ' + weekend.gender + ' #' + weekend.weekendNumber + ' attendee ' + attendee.migrationPersonId);
            return false;
        }

        var role = WeekendRolesByMigrationId[attendee.migrationRoleId];
        if (!role) {
            console.log('WARN: Cannot resolve role for Weekend ' + weekend.gender + ' #' + weekend.weekendNumber + ' attendee ' + attendee.migrationPersonId);
            return false;
        }

        attendee.person = {
            firstName: person.firstName,
            preferredName: person.preferredName,
            lastName: person.lastName,
            _id: person._id
        };

        attendee.role = {
            _id: role._id,
            title: role.title
        }

        delete attendee.migrationPersonId;
        delete attendee.migrationRoleId;

        person.experiences = person.experiences || [];
        person.experiences.push({
            roleTitle: role.title,
            weekendNumber: weekend.weekendNumber,
            weekendGender: weekend.gender
        });
        return true;
    });
}

function transformInactiveAddresses(addresses) {
    return transformAddresses(addresses, 'inactive');
}

function transformMaleExperience(data) {
    return experiencesTransformer.transform('Male', data);
}

function transformFemaleExperience(data) {
    return experiencesTransformer.transform('Female', data);
}

function transformTables(tables) {
    return tables.map(function (table) {
        return {
            _id: Random.id(),
            name: table['Table Name'],
            gender: table['MensLadies']
        }
    });
}

function addPeopleToCandidateWeekends(person, weekends) {
    if (!person.candidateOn) {
        return;
    }

    var attendance = {
        person: {
            firstName: person.firstName,
            preferredName: person.preferredName,
            lastName: person.lastName,
            _id: person._id
        },
        role: {
            _id: jobTransformer.getCandidateRoleId(),
            title: 'Candidate' 
        },
        isConfirmed: true,
        didAttend: true
    };

    var discriminator = _.capitalize(person.gender);
    var weekend = _.find(weekends, {gender: discriminator, weekendNumber: person.candidateOn});
    if (!weekend) {
        weekend = experiencesTransformer.makeWeekend(discriminator, person.candidateOn);
        weekends.push(weekend);
    }
    weekend.attendees.push(attendance);
}