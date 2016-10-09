
var db = require('odbc')();

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

console.log('provider string: ', cn);

var output = {};

const tables = [
    {name: 'Addresses', collection: 'people', transform: transformAddresses}, 
    {name: 'Table Names', collection: 'tables'},
    {name: 'Team Jobs', collection: 'weekend-roles', transform: transformJobs}, 
    {name: 'ExperienceMale', collection: 'weekends-male', transform: transformMaleExperience}, 
    {name: 'ExperienceFemale', collection: 'weekends-female', transform: transformFemaleExperience}
];

db.openAsync(cn)
    .then(function() {  
    	console.log('Connected!');
        return get(db, tables);
    })
    .then(function () {
        return db.closeAsync();
    })
    .catch(function(err) {
        console.log('Caught err: ', err);
    });

function get(db, tables) {
    return Promise.all(tables.map(function (table) {
        return db.queryAsync('SELECT * FROM `' + table.name + '`')
            .then(function (data) {
                if (table.transform) {
                    data = table.transform(data);
                }

                console.log('First for ' + table.collection + ':', data[0]);

                output[table.collection] = data;
                fs.writeFileSync(table.collection + '.json', JSON.stringify(data));
                console.log('Wrote ' + table.collection + '.json');
            });
    })).then(function () {
        var weekends = [];
        _.forEach(Weekends.Male, function (weekend, weekendNumber) {
            resolvePeopleAndRoles(weekend);
            weekends.push(weekend);
        });
        _.forEach(Weekends.Female, function (weekend, weekendNumber) {
            resolvePeopleAndRoles(weekend);
            weekends.push(weekend);
        });
        fs.writeFileSync('weekends.json', JSON.stringify(weekends));
        console.log('Wrote weekends.json');
    });
}

function resolvePeopleAndRoles(weekend) {
    var resolved = weekend.attendees.map(function (attendee) {
        var person = PeopleByMigrationId[attendee.migrationPersonId];
        if (person) {
            attendee.personId = person._id;
        }
        delete attendee.migrationPersonId;

        var role = WeekendRolesByMigrationId[attendee.migrationRoleId];
        if (role) {
            attendee.roleId = role._id;
        }
        delete attendee.migrationRoleId;
        return attendee;
    });
    weekend.attendees = resolved;
}

var PeopleByMigrationId = {};
function transformAddresses(addresses) {
    var Churches = [];
    var People = [];
    addresses.forEach(function (address) {
        var male = parse('Male', address, 'XL');
        var female = parse('Female', address, 'M');
        if (male) {
            if (female) {
                male.spouse = female._id;
                female.spouse = male._id;
            }
            People.push(male);
        }
        if (female) {
            People.push(female);
        }
    });

    People.forEach(function resolveSponsor(person) {
        person.migrationSponsorSearch.some(function (migrationSponsorId) {
            var sponsor = PeopleByMigrationId[migrationSponsorId];
            if (sponsor) {
                person.sponsorId = sponsor._id;
                console.log(sponsor.firstName + ' ' + sponsor.lastName + ' sponsored ' + person.firstName + ' ' + person.lastName);
                return true;
            }
            return false;
        });
        delete person.migrationSponsorSearch;
    });

    fs.writeFileSync('Churches.json', JSON.stringify(Churches.map(function (Church) {
        return {
            _id: Church._id,
            location: {
                label: Church.label
            }
        };
    })));

    return People;

    function parse(discriminator, address, defaultSize) {
        var firstName = address[discriminator + ' First Name'];
        if (firstName) {
            var person = {
                _id: Random.id(),
                firstName: firstName,
                lastName: address['LastName'], 
                gender: discriminator.toLowerCase(),
                isPastor: address['Pastor'],
                address: {
                    street: address.Address,
                    city: address.City,
                    state: address.StateOrProvince,
                    country: 'USA',
                    zip: address.PostalCode,
                    label: 'Home'
                }
            };
            assignChurch(person, address);
            maybeSet(person, address, [
                {name: discriminator + ' Pref Name', property: 'preferredName'},
                {name: discriminator + ' DOB', property: 'birthDate'}
            ]);
            insertStatus(person, address, discriminator);
            insertSponsorId(person, address, discriminator);
            insertPhones(person, address, discriminator);
            insertEmails(person, address, discriminator);
            insertShirtSize(person, address[discriminator + 'ShirtSize'], defaultSize);
            PeopleByMigrationId[address.AddressID + discriminator] = person;
            
            return person;
        }
    }

    function insertStatus(person, address, discriminator) {
        switch (address['Status ' + discriminator]) {
            case 1:
            case 2:
            case 3:
            case 4:
            case 5:
            case 6:
            case 7:
            case 13:
                person.status = 'candidate';
                break;
            case 8:
            case 9:
            case 10:
                person.status = 'active';
                break;
            case 12:
                person.status = 'inactive';
                break;
            case 15:
                person.status = 'deceased';
                break;
            case 11:
            case 14:
            case 0:
            default:
                person.status = 'not affiliated';
                break;
        }
    }

    function insertSponsorId(person, address, discriminator) {
        person.migrationSponsorSearch = [];
        if (discriminator === 'Female' && address['Sponsor Female ID#']) {
            person.migrationSponsorSearch.push(address['Sponsor Female ID#'] + 'Female');
        }

        if (address['Sponsor ID#']) {
            person.migrationSponsorSearch.push(address['Sponsor ID#'] + 'Male');
            person.migrationSponsorSearch.push(address['Sponsor ID#'] + 'Female');
        }
    }

    function assignChurch(person, address) {
        if (address.Church && address.Church !== null) {
            var church = _.find(Churches, {'label': address.Church});
            if (!church) {
                var newChurch = {
                    _id: Random.id(),
                    label: address.Church
                };
                Churches.push(newChurch);
            } else {
                person.churchId = church._id;
            }
        }
    }

    function maybeSet(person, address, fields) {
        fields.forEach(function (field) {
            var data = address[field.name];
            if (data && data !== null) {
                person[field.property] = data;
            }
        })
    }

    function insertShirtSize(person, shirtSize, defaultSize) {
        if (!shirtSize || shirtSize === null) {
            person.shirtSize = defaultSize;
            return;
        }

        if (shirtSize === 'Large') {
            person.shirtSize = 'L';
        } else if (shirtSize === 'Medium') {
            person.shirtSize = 'M';
        } else if (shirtSize === 'Small') {
            person.shirtSize = 'S';
        } else {
            person.shirtSize = shirtSize;
        }
    }

    function insertPhones(person, address, discriminator) {
        person.phoneNumbers = [];
        var hasPreferred = false;
        [
            {name: discriminator + ' Cell Phone', label: 'cell', canTxt: true}, 
            {name: discriminator + ' Work Phone', label: 'work', canTxt: false}, 
            {name: 'Home Phone', label: 'home', canTxt: false}
        ].forEach(function (field) {
            if (!address[field.name] || address[field.name] === null) {
                return;
            }
            var phoneNumber = {
                digits: formatPhone(address[field.name]),
                isPreferred: true,
                canTxt: field.canTxt,
                label: field.label
            };
            if (!hasPreferred) {
                phoneNumber.isPreferred = true;
                hasPreferred = true;
            }
            person.phoneNumbers.push(phoneNumber);
        });

        function formatPhone(digits) {
            return digits.replace(/\D/g,'');
        }
    }

    function insertEmails(person, address, discriminator) {
        person.emails = [];
        var fieldName = 'Email ' + discriminator;
        if (address[fieldName] !== null) {
            person.emails.push({
                address: address[fieldName],
                isPreferred: true
            });
        }
    }
}

function transformMaleExperience(data) {
    return transformExperience('Male', data);
}

function transformFemaleExperience(data) {
    return transformExperience('Female', data);
}

function transformExperience(discriminator, experiences) {
    buildWeekends(discriminator, experiences);

    return experiences.map(function (experience) {
        var attendance = {
            migrationPersonId: experience.AddressID + discriminator,
            migrationRoleId: experience.JobID,
            isConfirmed: true,
            didAttend: true
        };

        var weekendNumber = experience['BTD#'];
        if (weekendNumber) {
            Weekends[discriminator][weekendNumber].attendees.push(attendance);
        }
        return attendance;
    });
}

var WeekendRolesByMigrationId = {};
function transformJobs(jobs) {
    return jobs.map(function (job) {
        var role = {
            _id: Random.id(),
            migrationId: job.ID,
            title: job.Job
        };
        role.isHead = contains(job.Job, ['Head', 'Rover', 'Rector']);
        role.isProfessor = contains(job.Job, ['Prof']);
        
        WeekendRolesByMigrationId[role.migrationId] = role;
        return role;
    });
}

function contains(str, searches) {
    return searches.some(function (search) {
        return str.indexOf(search) !== -1;
    });
}

var Weekends = {Male: {}, Female: {}};
function buildWeekends(discriminator, experiences) {
    experiences.forEach(function (experience) {
        var weekendNumber = experience['BTD#'];
        if (weekendNumber && !Weekends[discriminator][weekendNumber]) {
            Weekends[discriminator][weekendNumber] = {
                _id: Random.id(),
                community: 'Birmingham Tres Dias',
                gender: discriminator,
                weekendNumber: weekendNumber,
                attendees: []
            };
            console.log('added weekend ' + discriminator + ' ' + weekendNumber);
        }
    });
}