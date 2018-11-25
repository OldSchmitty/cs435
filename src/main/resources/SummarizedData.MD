## Created Files

### PlayerAccountData.csv

This file contains the steam users account statistics on:
* `timecreated` - The date the user created the account
* `NumberOfGamesOwned` - the number of games the account owns
* `NetWorth` - The net worth of the account
* `TotalPlayTime` - The total play time for all games on the account

##### CSV Header
steamid,timecreated,NumberOfGamesOwned,NetWorth,TotalPlayTime


### PlayerGamesAndGenres.parquet
This file contains info on the games each SteamID. They need to be in parquet format since the games list is in an array.
* `NumberOfGamesOwned` - The number of games the steam user owns
* `Games` - The appId of all the games the steam user has in list form
* There are also a count of the number of games of each of the genres the steam users owns

##### Columns
steamid,NumberOfGamesOwned,Games,'Accounting','Action','Adventure','Animation&Modeling','AudioProduction','Casual','Design&Illustration','EarlyAccess','Education','FreetoPlay','Indie','MassivelyMultiplayer','PhotoEditing','RPG','Racing','Simulation','SoftwareTraining','Sports','Strategy','Utilities','VideoProduction','WebPublishing'


### GroupPlayerStatistics.csv
This file contains a groups players statistics
* `NumberOfMembers` - The number of users in the group
* `AverageMemberNetWorth` - The average net worth for all the users in the group
* `NetWorthOfGroup` - The total net worth of all the users in the group
* `AverageNumberOfGames` - The average number of games each user in the group own
* `TotalNumberOfGames` - The total game all the users in the group own
* `AverageTotalPlayTime` - The average total playtime of each user
* `TotalPlayTime` - The total playtime of all the users in the group

##### CSV Header
groupid,NumberOfMembers,AverageMemberNetWorth,NetWorthOfGroup,AverageNumberOfGames,TotalNumberOfGames,AverageTotalPlayTime,TotalPlayTime


### UsersNumberOfGroups.csv
This file contains basic info on the SteamID's statistics
* `NumberOfGroups` - The number of groups the SteamID is in
* `NumberOfGamesOwned` - The number of games the SteamID owns
* `TotalPlayTime` - The total playtime of the SteamID
* `NetWorth` - The net worth of the SteamID

##### CSV Header
steamid,NumberOfGroups,NumberOfGamesOwned,TotalPlayTime,NetWorth

### GroupsPlayers.parquet
This file contains all the players that belong to each GroupId.They need to be in parquet format since the SteamID list is in an array.
* `Members` - A list of all members in the group
* `NumberOfMembers` - The total number of members in the group

##### Columns
groupid,Members,NumberOfMembers

### PlayersGroups.parquet
This file contains all the groups that a SteamID is in.They need to be in parquet format since the GroupIDs list is in an array.
* `groups` - A list of all groups a SteamID is in
* `NumberOfGroups` - The total number of groups a SteamID is in

##### Columns
steamid,groups,NumberOfGroups



