
// Query 7
// Return the number of users born in each month as a new collection
// named countbymonth that has the following schema
//
// month: xxx
// borncount: xxx
// 
// You need to use aggregate for an acceptable solution.
// You will likely find it useful to use the following 
// elements in the aggregate pipeline:
// $group
// $sort
// $addfields: to add a column called MOB
// $project: you may need it to remove _id created by group
// $out: to output the result to the new collection.
//


function create_count_by_month(dbname) {
    db = db.getSiblingDB(dbname);

    db.users.aggregate([
        {
            $group: {
                _id: "$MOB",
                borncount: { $sum: 1 }
            }
        },
        { 
            $sort: { _id: 1 }
        },
        {
            $addFields: { 
                MOB: "$_id"
            }
        },
        {
            $project: {
                _id: 0,
                MOB: 1,
                borncount: 1
            }
        },
        { 
            $out: "countbymonth"
        }
    ]);
}


