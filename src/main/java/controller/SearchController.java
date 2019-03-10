package controller;
import java.util.HashMap;
import java.util.List;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import parta.SearchLuceneIndex;
import service.SearchService;


@Path("/search")
public class SearchController {

    SearchService searchService=new SearchService();

//    @GET
//    @Produces(MediaType.APPLICATION_JSON)
//    public List documentCount()
//    {
//
//        List documentCount=searchService.getDocumentCount();
//        return documentCount;
//    }

    @GET
    @Path("/{searchterm}")
    @Produces(MediaType.APPLICATION_JSON)
    public HashMap<String, String> getSearchResults(@PathParam("searchterm") String searchterm)
    {
        return searchService.getSearchResults(searchterm);
    }

//    @POST
//    @Produces(MediaType.APPLICATION_JSON)
//    public Country addCountry(Country country)
//    {
//        return countryService.addCountry(country);
//    }

//    @PUT
//    @Produces(MediaType.APPLICATION_JSON)
//    public Country updateCountry(Country country)
//    {
//        return countryService.updateCountry(country);
//
//    }
//
//    @DELETE
//    @Path("/{id}")
//    @Produces(MediaType.APPLICATION_JSON)
//    public void deleteCountry(@PathParam("id") int id)
//    {
//        countryService.deleteCountry(id);
//
//    }

}
