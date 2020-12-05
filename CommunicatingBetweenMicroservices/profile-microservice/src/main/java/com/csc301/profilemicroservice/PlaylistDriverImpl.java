package com.csc301.profilemicroservice;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.databind.ObjectMapper;

import okhttp3.Call;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;

import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;

@Repository
public class PlaylistDriverImpl implements PlaylistDriver {

	Driver driver = ProfileMicroserviceApplication.driver;

	public static void InitPlaylistDb() {
		String queryStr;

		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				queryStr = "CREATE CONSTRAINT ON (nPlaylist:playlist) ASSERT exists(nPlaylist.plName)";
				trans.run(queryStr);
				trans.success();
			}
			session.close();
		}
	}

	@Override
	public DbQueryStatus likeSong(String userName, String songId) {
		try {
		boolean createSong = true;
		DbQueryStatus notFound = new DbQueryStatus("userName or song not found",DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
		
		DbQueryStatus successStatus = new DbQueryStatus("OK",DbQueryExecResult.QUERY_OK);
		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				StatementResult checker = trans.run("MATCH(n:profile) WHERE EXISTS(n.userName) and n.userName = $x RETURN n",Values.parameters("x", userName));
				if(!checker.hasNext()) {
					
					return notFound;
				}
				
				
				StatementResult checker2 = trans.run("MATCH(s:song) WHERE EXISTS(s.songId) and s.songId = $x RETURN s",
						Values.parameters("x", songId));
				if(checker2.hasNext()) {
					createSong = false;
				}
				
			}
			session.close();
		}
		
		OkHttpClient client = new OkHttpClient();

		Request request = new Request.Builder()
		                     .url("http://localhost:3001/getSongById/" + songId)
		                     .build();
		
		
			Response response = client.newCall(request).execute();
			JSONObject deserialized = new JSONObject(response.body().string());
			response.close();
			if(!deserialized.getString("status").equals("OK")) {
				return notFound;
			}

			
			
			
			try(Session session = driver.session()){
				if(createSong) {
					session.run("CREATE (s:song {songId:$x})", Values.parameters("x", songId));
				}
				
				session.run("MATCH (pl:playlist {plName:$x})," + "(s:song {songId:$y})\n" + "MERGE (pl)-[r:includes]->(s)\n" + "RETURN r",
					Values.parameters("x", userName+"-favorites", "y", songId));
				session.close();
				
			
		}
			  RequestBody body = RequestBody.create(null, new byte[0]);;
		   
			HttpUrl.Builder urlBuilder = HttpUrl.parse("http://localhost:3001/updateSongFavouritesCount/" + songId).newBuilder();
			urlBuilder.addQueryParameter("shouldDecrement", "false");
			
			String url = urlBuilder.build().toString();

			Request secondRequest = new Request.Builder()
			                     .url(url)
			                     .method("PUT", body)
			                     .build();
			
			Response secondResponse = client.newCall(secondRequest).execute();	
			
			
		
			
		}catch(Exception e) {
		return new DbQueryStatus("ERROR",DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
		

		return new DbQueryStatus("OK",DbQueryExecResult.QUERY_OK);
	}

	@Override
	public DbQueryStatus unlikeSong(String userName, String songId) {
		
		try {
			boolean createSong = true;
			DbQueryStatus notFound = new DbQueryStatus("NOT FOUND",DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
			
			DbQueryStatus successStatus = new DbQueryStatus("OK",DbQueryExecResult.QUERY_OK);
			try (Session session = ProfileMicroserviceApplication.driver.session()) {
				try (Transaction trans = session.beginTransaction()) {
					StatementResult checker = trans.run("MATCH(n:profile) WHERE EXISTS(n.userName) and n.userName = $x RETURN n",Values.parameters("x", userName));
					if(!checker.hasNext()) {
						
						return notFound;
					}
					
					
					StatementResult checker2 = trans.run("MATCH(s:song) WHERE EXISTS(s.songId) and s.songId = $x RETURN s",
							Values.parameters("x", songId));
					
					StatementResult checker3 = trans.run("MATCH (pl:playlist)-[r:includes]->(s:song) WHERE pl.plName = $x and s.songId = $y RETURN r",
							Values.parameters("x",userName+"-favorites","y",songId));
					if(!checker3.hasNext()) {
						return notFound;
					}
					
					
				}
				session.close();
			}
			
			
			try(Session session = driver.session()){
				
				session.run("MATCH (pl:playlist)-[r:includes]->(s:song) WHERE pl.plName = $x and s.songId = $y DELETE r",
					Values.parameters("x", userName+"-favorites", "y", songId));
				session.close();
					
		}
			
			
			  RequestBody body = RequestBody.create(null, new byte[0]);
			   
				HttpUrl.Builder urlBuilder = HttpUrl.parse("http://localhost:3001/updateSongFavouritesCount/" + songId).newBuilder();
				urlBuilder.addQueryParameter("shouldDecrement", "true");
				
				String url = urlBuilder.build().toString();

				Request secondRequest = new Request.Builder()
				                     .url(url)
				                     .method("PUT", body)
				                     .build();
				OkHttpClient client = new OkHttpClient();
				Response secondResponse = client.newCall(secondRequest).execute();	
				
			
			
			
			
			return successStatus;
		}catch(Exception e) {
			return new DbQueryStatus("Error",DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
		}
	}

	@Override
	public DbQueryStatus deleteSongFromDb(String songId) {
		try {
		try (Session session = ProfileMicroserviceApplication.driver.session()) {
			try (Transaction trans = session.beginTransaction()) {
				StatementResult checker = trans.run("MATCH(s:song) WHERE EXISTS(s.songId) and s.songId = $x RETURN s",
						Values.parameters("x", songId));
				if(!checker.hasNext()) {
					return new DbQueryStatus("NOT FOUND", DbQueryExecResult.QUERY_ERROR_NOT_FOUND);
				}
				
			}
			session.close();
		}
		
		
		try(Session session = driver.session()){
			
			session.run("MATCH(s:song{songId:$x}) detach delete s",
				Values.parameters("x", songId));
			session.close();
				
	}
		 RequestBody body = RequestBody.create(null, new byte[0]);
		   
		 OkHttpClient client = new OkHttpClient();

			Request request = new Request.Builder()
			                     .url("http://localhost:3001/deleteSongById/" + songId)
			                     .method("DELETE",body)
			                     .build();
			
			
				Response response = client.newCall(request).execute();
				response.close();
				return new DbQueryStatus("OK",DbQueryExecResult.QUERY_OK);
		
		}catch(Exception e) {
			System.out.println(e);
			return new DbQueryStatus("Error",DbQueryExecResult.QUERY_ERROR_GENERIC);
		}
		
	
	}
}
