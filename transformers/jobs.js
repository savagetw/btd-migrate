'use strict';

var Random = require('meteor-random');

var candidateRoleId = Random.id();

module.exports = {
    transform(jobs) {
        var transformedJobs = jobs.map(function (job) {
            var role = {
                _id: Random.id(),
                migrationId: job.ID,
                title: job.Job
            };
            role.isHead = contains(job.Job, ['Head', 'Rover', 'Rector']);
            role.isProfessor = contains(job.Job, ['Prof']);
            return role;
        });

        transformedJobs.push({
            _id: candidateRoleId,
            title: 'Candidate',
            isHead: false,
            isProfessor: false
        });

        return [
            {name: 'WeekendRoles', collection: transformedJobs}
        ];
    },
    getCandidateRoleId() {
        return candidateRoleId;
    }
};

function contains(str, searches) {
    return searches.some(function (search) {
        return str.indexOf(search) !== -1;
    });
}