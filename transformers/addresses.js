'use strict';

var _ = require('lodash');
var Random = require('meteor-random');

module.exports = function transformAddresses(addresses, statusOverride) {
    var People = [];
    addresses.forEach(function (address) {
        var male = parse('Male', address, 'XL');
        var female = parse('Female', address, 'M');
        if (male) {
            if (female) {
                male.spouse = {
                    firstName: female.firstName,
                    preferredName: female.preferredName,
                    lastName: female.lastName,
                    _id: female._id
                };

                female.spouse = {
                    firstName: male.firstName,
                    preferredName: male.preferredName,
                    lastName: male.lastName,
                    _id: male._id
                };
            }
            People.push(male);
        }
        if (female) {
            People.push(female);
        }
    });

    return [
        {name: 'People', collection: People}
    ];

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
                },
                candidateOn: getWeekendNumber(discriminator, address)
            };
            maybeSet(person, address, [
                {name: discriminator + ' Pref Name', property: 'preferredName'},
                {name: discriminator + ' DOB', property: 'birthDate'},
                {name: 'Church', property: 'church'}
            ]);
            if (statusOverride) {
                person.status = statusOverride;
            } else {
                insertStatus(person, address, discriminator);
            }
            insertSponsor(person, address, discriminator);
            insertPhones(person, address, discriminator);
            insertEmails(person, address, discriminator);
            insertShirtSize(person, address[discriminator + 'ShirtSize'], defaultSize);

            person.migrationId = address.AddressID + discriminator;
            
            return person;
        }
    }

    function getWeekendNumber(discriminator, address) {
        var fieldName = discriminator + 'Weekend';
        return address[fieldName + '#'] || address[fieldName + ' #'];
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

    function insertSponsor(person, address, discriminator) {
        person.migrationSponsorSearch = [];
        if (discriminator === 'Female' && address['Sponsor Female ID#']) {
            person.migrationSponsorSearch.push(address['Sponsor Female ID#'] + 'Female');
        }

        if (address['Sponsor ID#']) {
            var maleSponsor = address['Sponsor ID#'] + 'Male';
            var femaleSponsor = address['Sponsor ID#'] + 'Female';
            
            if (discriminator === 'Female') {
                person.migrationSponsorSearch.push(femaleSponsor);
                person.migrationSponsorSearch.push(maleSponsor);
            } else {
                person.migrationSponsorSearch.push(maleSponsor);
                person.migrationSponsorSearch.push(femaleSponsor);
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
                isPreferred: false,
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
};