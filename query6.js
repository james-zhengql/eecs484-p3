// Query 6
// Find the average friend count per user.
// Return a decimal value as the average user friend count of all users in the users collection.

function average_friends(dbname) {
    db = db.getSiblingDB(dbname);

    // TODO: calculate the average friend count
    let totalFriends = 0;
    let totalUsers = 0;

    db.users.find().forEach(user => {
        let friendCount = user.friends ? user.friends.length : 0; 
        totalFriends += friendCount; 
        totalUsers += 1; 
    });

    let averageFriends = totalFriends / totalUsers;

    return averageFriends;
}