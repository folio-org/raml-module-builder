package org.folio.rest.tools.utils;

import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.Link.Builder;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

/**
 * @author shale
 *
 */
public class ResponseImpl extends Response {

  int status = 0;

  public void setStatus(int status){
    this.status = status;
  }

  @Override
  public int getStatus() {
    // TODO Auto-generated method stub
    return status;
  }

  @Override
  public StatusType getStatusInfo() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getEntity() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T readEntity(Class<T> entityType) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T readEntity(GenericType<T> entityType) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T readEntity(Class<T> entityType, Annotation[] annotations) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T readEntity(GenericType<T> entityType, Annotation[] annotations) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean hasEntity() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean bufferEntity() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
  }

  @Override
  public MediaType getMediaType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Locale getLanguage() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getLength() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Set<String> getAllowedMethods() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, NewCookie> getCookies() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public EntityTag getEntityTag() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getDate() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getLastModified() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public URI getLocation() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Set<Link> getLinks() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean hasLink(String relation) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Link getLink(String relation) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Builder getLinkBuilder(String relation) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MultivaluedMap<String, Object> getMetadata() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public MultivaluedMap<String, String> getStringHeaders() {
    // TODO Auto-generated method stub
    return new MultivaluedHashMap<>();
  }

  @Override
  public String getHeaderString(String name) {
    // TODO Auto-generated method stub
    return null;
  }

}
