'use strict';

var _ = require('lodash');
var Random = require('meteor-random');

module.exports = {
    transform(discriminator, experiences) {
        var weekends = buildWeekends(discriminator, experiences);

        var Experiences = experiences.map(function (experience) {
            var attendance = {
                migrationPersonId: experience.AddressID + discriminator,
                migrationRoleId: experience.JobID,
                isConfirmed: true,
                didAttend: true
            };

            var weekendNumber = experience['BTD#'];
            if (weekendNumber) {
                _.find(weekends, {weekendNumber: weekendNumber}).attendees.push(attendance);
            }
            return attendance;
        });

        return [
            {name: 'Experiences', collection: Experiences},
            {name: 'Weekends', collection: weekends}
        ];
    },
    makeWeekend(gender, weekendNumber) {
        return new Weekend(gender, weekendNumber);
    }
};

function buildWeekends(discriminator, experiences) {
    var weekends = [];
    experiences.forEach(function (experience) {
        var weekendNumber = experience['BTD#'];
        if (!weekendNumber) {
            return;
        }

        var weekend = _.find(weekends, {weekendNumber: weekendNumber});
        if (!weekend) {
            weekends.push(new Weekend(discriminator, weekendNumber));
        }
    });
    return weekends;
}

function Weekend(gender, weekendNumber_) {
    this._id = Random.id();           
    this.community = 'Birmingham Tres Dias';
    this.gender = gender;
    this.weekendNumber = weekendNumber_;
    this.attendees = [];
}
