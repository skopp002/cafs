package service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import parta.SearchLuceneIndex;


/*
 * It is just a helper class which should be replaced by database implementation.
 * It is not very well written class, it is just used for demonstration.
 */
public class SearchService {

    static SearchLuceneIndex s = new SearchLuceneIndex();


    public SearchService() {
        super();
    }

    public HashMap<String, String> getSearchResults(String searchTerm) {

        s.setSearchTerm(searchTerm);
        HashMap<String, String> searchResults = s.searchLuceneIndex();
        return searchResults;
    }
}

//    public String getDocument(int id)
//    {
//        String country= countryIdMap.get(id);
//
//        if(country == null)
//        {
//            throw new CountryNotFoundException("Country with id "+id+" not found");
//        }
//        return country;
//    }
//    public Country addCountry(Country country)
//    {
//        country.setId(getMaxId()+1);
//        countryIdMap.put(country.getId(), country);
//        return country;
//    }
//
//    public Country updateCountry(Country country)
//    {
//        if(country.getId()<=0)
//            return null;
//        countryIdMap.put(country.getId(), country);
//        return country;
//
//    }
//    public void deleteCountry(int id)
//    {
//        countryIdMap.remove(id);
//    }

//    public static HashMap<Integer, Country> getDocument() {
//        return countryIdMap;
//    }

    // Utility method to get max id
//    public static int getMaxId()
//    {   int max=0;
//        for (int id:countryIdMap.keySet()) {
//            if(max<=id)
//                max=id;
//
//        }
//
//        return max;
//    }
