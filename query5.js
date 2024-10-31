// Query 5
// Find the oldest friend for each user who has a friend. For simplicity,
// use only year of birth to determine age, if there is a tie, use the
// one with smallest user_id. You may find query 2 and query 3 helpful.
// You can create selections if you want. Do not modify users collection.
// Return a javascript object : key is the user_id and the value is the oldest_friend id.
// You should return something like this (order does not matter):
// {user1:userx1, user2:userx2, user3:userx3,...}

// function oldest_friend(dbname) {
//     db = db.getSiblingDB(dbname);

//     let results = {}; 

//     db.users.find().forEach(user => {
//         const oldestFriend = db.flat_users
//             .find({ $or: [{ user_id: user.user_id }, { friends: user.user_id }] })
//             .sort({ YOB: 1, user_id: 1 })
//             .limit(1)
//             .toArray();
//         if (oldestFriend.length > 0) {
//             results[user.user_id] = oldestFriend[0].user_id;
//         }
//     });
    

//     return results;
// }
function oldest_friend(dbname) {
    db = db.getSiblingDB(dbname);

    let results = {};
    let friendships = {};

    db.users.aggregate([
        { $unwind: "$friends" },
        { $project: { _id: 0, user_id: 1, friend: "$friends" } }
    ]).forEach(rel => {
        if (!friendships[rel.user_id]) {
            friendships[rel.user_id] = [];
        }
        friendships[rel.user_id].push(rel.friend);
    });

    db.users.aggregate([
        { $unwind: "$friends" },
        { $project: { _id: 0, user_id: "$friends", friend: "$user_id" } }
    ]).forEach(rel => {
        if (!friendships[rel.user_id]) {
            friendships[rel.user_id] = [];
        }
        friendships[rel.user_id].push(rel.friend);
    });


    for (var user_id in friendships) {
        const oldestFriend = db.users
            .find({ user_id: { $in: friendships[user_id] } })
            .sort({ YOB: 1, user_id: 1 }) 
            .limit(1)
            .toArray();

        if (oldestFriend.length > 0) {
            results[user_id] = oldestFriend[0].user_id;
        }
    }

    return results;
}


