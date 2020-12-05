package com.csc301.profilemicroservice;




import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;

import org.springframework.stereotype.Repository;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;

@Repository
public class ProfileDriverImpl implements ProfileDriver {

	Driver driver = ProfileMicroserviceApplication.driver;

	public static void InitProfileDb() {
		String queryStr;

		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				queryStr = "CREATE CONSTRAINT ON (nProfile:profile) ASSERT exists(nProfile.userName)";
				trans.run(queryStr);

				queryStr = "CREATE CONSTRAINT ON (nProfile:profile) ASSERT exists(nProfile.password)";
				trans.run(queryStr);

				queryStr = "CREATE CONSTRAINT ON (nProfile:profile) ASSERT nProfile.userName IS UNIQUE";
				trans.run(queryStr);

				trans.success();
			}
			session.close();
		}
	}
	
	@Override
	public DbQueryStatus createUserProfile(String userName, String fullName, String password) {
		try {
			
			if(userName.equals("")|| fullName.equals("") || password.equals("")) {
				return new DbQueryStatus("Missing info",DbQueryExecResult.QUERY_ERROR_GENERIC);
			}
			
		DbQueryStatus alreadyExistsStatus = new DbQueryStatus("User name already taken",DbQueryExecResult.QUERY_ERROR_GENERIC);
		DbQueryStatus successStatus = new DbQueryStatus("OK",DbQueryExecResult.QUERY_OK);
		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				StatementResult checker = trans.run("MATCH(n:profile) WHERE EXISTS(n.userName) and n.userName = $x RETURN n",Values.parameters("x", userName));
				if(checker.hasNext()) {
					
					return alreadyExistsStatus;
				}
			}
			session.close();
		}
		try(Session session = driver.session()){
			
				session.run("CREATE (p:profile {userName:$x,password:$y,fullName:$z})", Values.parameters("x", userName, "y", password,"z",fullName));
				session.run("CREATE (p:playlist {plName:$x})", Values.parameters("x", userName+"-favorites"));
				session.run("MATCH (p:profile {userName:$x})," + "(pl:playlist {plName:$y})\n" + "MERGE (p)-[r:created]->(pl)\n" + "RETURN r",
					Values.parameters("x", userName, "y", userName+"-favorites"));
				session.close();
				return successStatus;
			
		}
		}catch(Exception e) {
		
			return new DbQueryStatus("Error",DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
	}

	
	
	@Override
	public DbQueryStatus followFriend(String userName, String frndUserName) {
		try {
		DbQueryStatus notFound = new DbQueryStatus("userName or frndUserName not found",DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
		DbQueryStatus successStatus = new DbQueryStatus("OK",DbQueryExecResult.QUERY_OK);
		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				StatementResult checker = trans.run("MATCH(n:profile) WHERE EXISTS(n.userName) and n.userName = $x RETURN n",Values.parameters("x", userName));
				if(!checker.hasNext()) {
					
					return notFound;
				}
				StatementResult secondChecker = trans.run("MATCH(n:profile) WHERE EXISTS(n.userName) and n.userName = $x RETURN n",Values.parameters("x", frndUserName));
				if(!secondChecker.hasNext()) {
					return notFound;
				}
			}
			session.close();
		}
		
		try(Session session = driver.session()){
			
			session.run("MATCH (p:profile {userName:$x})," + "(f:profile {userName:$y})\n" + "MERGE (p)-[r:follows]->(f)\n" + "RETURN r",
				Values.parameters("x", userName, "y", frndUserName));
			session.close();
			return successStatus;
		
	}}catch(Exception e) {
		
		return new DbQueryStatus("Error",DbQueryExecResult.QUERY_ERROR_GENERIC);
	}
		
		
		
		
		
		
	}

	@Override
	public DbQueryStatus unfollowFriend(String userName, String frndUserName) {
		try {
		
		DbQueryStatus notFound = new DbQueryStatus("userName or frndUserName not found",DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
		DbQueryStatus successStatus = new DbQueryStatus("Successful",DbQueryExecResult.QUERY_OK);
		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				StatementResult checker = trans.run("MATCH(n:profile) WHERE EXISTS(n.userName) and n.userName = $x RETURN n",Values.parameters("x", userName));
				if(!checker.hasNext()) {
					
					return notFound;
				}
				StatementResult secondChecker = trans.run("MATCH(n:profile) WHERE EXISTS(n.userName) and n.userName = $x RETURN n",Values.parameters("x", frndUserName));
				if(!secondChecker.hasNext()) {
					return notFound;
				}
				StatementResult thirdChecker = trans.run("MATCH (p:profile)-[r:follows]->(f:profile) WHERE p.userName = $x and f.userName = $y RETURN r",
						Values.parameters("x",userName,"y",frndUserName));
				if(!thirdChecker.hasNext()) {
					return new DbQueryStatus("Not following so cannot unfollow",DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
					
				}
			}
			
			
			session.close();
		}
		
		
		
		try(Session session = driver.session()){
			
			session.run("MATCH (p:profile)-[r:follows]->(f:profile) WHERE p.userName = $x and f.userName = $y DELETE r",
				Values.parameters("x", userName, "y", frndUserName));
			session.close();
			return successStatus;
		
	}}catch(Exception e) {
		return new DbQueryStatus("Error",DbQueryExecResult.QUERY_ERROR_GENERIC);
	}
			
	}

	@Override
	public DbQueryStatus getAllSongFriendsLike(String userName) {
		
		
		
		
		return null;
	}
}
