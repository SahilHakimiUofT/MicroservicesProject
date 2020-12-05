package com.csc301.songmicroservice;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import com.mongodb.DBCollection;
import com.mongodb.client.MongoCollection;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;


@Repository
public class SongDalImpl implements SongDal {

	private final MongoTemplate db;
	//private MongoCollection<Document> collection = db.getCollection("songs");
	
	

	@Autowired
	public SongDalImpl(MongoTemplate mongoTemplate) {
		this.db = mongoTemplate;
	}

	@Override
	public DbQueryStatus addSong(Song songToAdd) {
				
		if(songToAdd.KEY_SONG_NAME != null && songToAdd.KEY_SONG_ARTIST_FULL_NAME != null && songToAdd.KEY_SONG_ALBUM != null) {
			db.insert(songToAdd);
			
			if(checkAdd(songToAdd.getId())) {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_OK;
				String message = "OK";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				dbQueryStatus.setData(songToAdd.getJsonRepresentation());
				
				return dbQueryStatus;
				
			} else {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
				String message = "Fail to add the song";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				
				return dbQueryStatus;
			}
						
		} else {
			DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
			String message = "Missing Info";	
			DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
			
			return dbQueryStatus;
		}
		
	}
	
	public  boolean checkAdd(String songId) {
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").is(songId));
		List<Song> songs = db.find(query, Song.class);
		
		return (songs.size() == 1);
	}

	@Override
	public DbQueryStatus findSongById(String songId) {
		
		if(songId != null) {
			Query query = new Query();
			query.addCriteria(Criteria.where("_id").is(songId));			
			List<Song> songs = db.find(query, Song.class); 
			
			if(songs.size() == 1) {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_OK;
				String message = "OK";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				dbQueryStatus.setData(songs.get(0).getJsonRepresentation());
				
				return dbQueryStatus;
				
			} else if(songs.size() >= 1) {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
				String message = "More than 1 song share a same id";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				
				return dbQueryStatus;
				
			} else {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_NOT_FOUND;
				String message = "No song with this id";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				
				return dbQueryStatus;
				
			} 
		
		} else {
			DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
			String message = "id is null";
			DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
			
			return dbQueryStatus;
		}
	}

	@Override
	public DbQueryStatus getSongTitleById(String songId) {
		
		if(songId != null) {
			Query query = new Query();
			query.addCriteria(Criteria.where("_id").is(songId));			
			List<Song> songs = db.find(query, Song.class); 
			
			if(songs.size() == 1) {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_OK;
				String message = "OK";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				dbQueryStatus.setData(songs.get(0).getSongName());
				
				return dbQueryStatus;
				
			} else if(songs.size() >= 1) {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
				String message = "More than 1 song share a same id";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				
				return dbQueryStatus;
				
			} else {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_NOT_FOUND;
				String message = "No song with this id";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				
				return dbQueryStatus;
				
			} 
		
		} else {
			DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
			String message = "id is null";
			DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
			
			return dbQueryStatus;
		}
		
	}
	
	@Override
	public DbQueryStatus deleteSongById(String songId) {
		if(songId != null) {
			Query query = new Query();
			query.addCriteria(Criteria.where("_id").is(songId));			
			List<Song> songs = db.find(query, Song.class); 
			
			if(songs.size() == 1) {
				
				db.remove(query, Song.class);
				
				if(checkDelete(songId)) {
					try {
					DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_OK;
					String message = "OK";
					DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
					
					RequestBody body = RequestBody.create(null, new byte[0]);;
					   
					 OkHttpClient client = new OkHttpClient();

						Request request = new Request.Builder()
						                     .url("http://localhost:3002/deleteAllSongsFromDb/" + songId)
						                     .method("PUT",body)
						                     .build();
						
						
							Response response = client.newCall(request).execute();
							response.close();
					
					
					
					
					return dbQueryStatus;
					}catch(Exception e) {
						return new DbQueryStatus("Error",DbQueryExecResult.QUERY_ERROR_GENERIC);
					}
				} else {
					DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
					String message = "Fail to delete the Song";
					DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
					
					return dbQueryStatus;
				}
				
				
			} else if(songs.size() >= 1) {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
				String message = "More than 1 song share a same id";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				
				return dbQueryStatus;
				
			} else {
				DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_NOT_FOUND;
				String message = "No song with this id";
				DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
				
				return dbQueryStatus;
				
			} 
		
		} else {
			DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
			String message = "id is null";
			DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
			
			return dbQueryStatus;
		}
	}
	
	public boolean checkDelete(String songId) {
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").is(songId));
		List<Song> songs = db.find(query, Song.class);
		
		return (songs.size() == 0);
	}

	@Override
	public DbQueryStatus updateSongFavouritesCount(String songId, boolean shouldDecrement) {
		
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").is(songId));			
		List<Song> songs = db.find(query, Song.class); 
		
		if(songs.size() == 1) {
			
			if (shouldDecrement) {
				if(songs.get(0).getSongAmountFavourites() <= 0) {
					DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
					String message = "Song's Ammount Favourites <= 0";
					DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
					
					return dbQueryStatus;
				} else {
					long oldAmount = songs.get(0).getSongAmountFavourites();
					songs.get(0).setSongAmountFavourites(oldAmount - 1);
					db.save(songs.get(0));
					
					if(checkDecrement(oldAmount, songId)) {
						DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_OK;
						String message = "OK";
						DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
						//dbQueryStatus.setData(songs.get(0).getJsonRepresentation());
						
						return dbQueryStatus;
					} else {
						DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
						String message = "Fail to decrement the amount of faviurites";
						DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
						
						return dbQueryStatus;
					}
				}
			} else {
				long oldAmount = songs.get(0).getSongAmountFavourites();
				songs.get(0).setSongAmountFavourites(oldAmount + 1);
				
				db.save(songs.get(0));
				
				if(checkIncrement(oldAmount, songId)) {
					DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_OK;
					String message = "OK";
					DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
					//dbQueryStatus.setData(songs.get(0).getJsonRepresentation());
					
					return dbQueryStatus;
				} else {
					DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
					String message = "Fail to increment the amount of faviurites";
					DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
					
					return dbQueryStatus;
				}
			}			
		} else if(songs.size() >= 1) {
			DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_GENERIC;
			String message = "More than 1 song share a same id";
			DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
			
			return dbQueryStatus;
			
		} else {
			DbQueryExecResult dbQueryExecResult = DbQueryExecResult.QUERY_ERROR_NOT_FOUND;
			String message = "No song with this id";
			DbQueryStatus dbQueryStatus = new DbQueryStatus(message, dbQueryExecResult);
			
			return dbQueryStatus;
			
		} 
	}
	
	public boolean checkDecrement(long oldAmount, String songId) {
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").is(songId));			
		List<Song> songs = db.find(query, Song.class); 
		
		return(songs.get(0).getSongAmountFavourites() == oldAmount - 1);
	}
	
	public boolean checkIncrement(long oldAmount, String songId) {
		Query query = new Query();
		query.addCriteria(Criteria.where("_id").is(songId));			
		List<Song> songs = db.find(query, Song.class); 
		
		return(songs.get(0).getSongAmountFavourites() == oldAmount + 1);
	}
}