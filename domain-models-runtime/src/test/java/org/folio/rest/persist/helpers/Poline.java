
package org.folio.rest.persist.helpers;

import java.util.ArrayList;
import java.util.List;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.folio.rest.jaxrs.model.Metadata;


/**
 * Book schema
 * <p>
 * PO lines schema
 *
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "id",
    "edition",
    "checkin_items",
    "instance_id",
    "agreement_id",
    "purchase_order_id",
    "acquisition_method",
    "adjustment",
    "alerts",
    "cancellation_restriction",
    "cancellation_restriction_note",
    "claims",
    "collection",
    "cost",
    "count",
    "description",
    "details",
    "donor",
    "eresource",
    "fund_distribution",
    "location",
    "order_format",
    "owner",
    "payment_status",
    "physical",
    "po_line_description",
    "po_line_number",
    "po_line_workflow_status",
    "publication_date",
    "publisher",
    "receipt_date",
    "receipt_status",
    "reporting_codes",
    "requester",
    "rush",
    "selector",
    "source",
    "tags",
    "title",
    "vendor_detail",
    "metadata"
})
public class Poline {

    /**
     *
     * (Required)
     *
     */
    @JsonProperty("id")
    @NotNull
    private String id;
    @JsonProperty("edition")
    private String edition;
    @JsonProperty("checkin_items")
    private Boolean checkinItems;
    @JsonProperty("instance_id")
    private String instanceId;
    @JsonProperty("agreement_id")
    private String agreementId;
    @JsonProperty("purchase_order_id")
    private String purchaseOrderId;
    @JsonProperty("acquisition_method")
    private String acquisitionMethod;
    @JsonProperty("adjustment")
    private String adjustment;
    @JsonProperty("alerts")
    @Valid
    private List<Object> alerts = new ArrayList<Object>();
    @JsonProperty("cancellation_restriction")
    private String cancellationRestriction;
    @JsonProperty("cancellation_restriction_note")
    private String cancellationRestrictionNote;
    @JsonProperty("claims")
    @Valid
    private List<String> claims = new ArrayList<String>();
    @JsonProperty("collection")
    private Boolean collection;
    @JsonProperty("cost")
    private String cost;
    @JsonProperty("count")
    private Integer count;
    @JsonProperty("description")
    private String description;
    @JsonProperty("details")
    private String details;
    @JsonProperty("donor")
    private String donor;
    @JsonProperty("eresource")
    private String eresource;
    @JsonProperty("fund_distribution")
    @Valid
    private List<String> fundDistribution = new ArrayList<String>();
    @JsonProperty("location")
    private String location;
    @JsonProperty("order_format")
    private String orderFormat;
    @JsonProperty("owner")
    private String owner;
    @JsonProperty("payment_status")
    private String paymentStatus;
    @JsonProperty("physical")
    private String physical;
    @JsonProperty("po_line_description")
    private String poLineDescription;
    @JsonProperty("po_line_number")
    private String poLineNumber;
    @JsonProperty("po_line_workflow_status")
    private String poLineWorkflowStatus;
    @JsonProperty("publication_date")
    private String publicationDate;
    @JsonProperty("publisher")
    private String publisher;
    @JsonProperty("receipt_date")
    private String receiptDate;
    @JsonProperty("receipt_status")
    private String receiptStatus;
    @JsonProperty("reporting_codes")
    @Valid
    private List<String> reportingCodes = new ArrayList<String>();
    @JsonProperty("requester")
    private String requester;
    @JsonProperty("rush")
    private Boolean rush;
    @JsonProperty("selector")
    private String selector;
    @JsonProperty("source")
    private String source;
    @JsonProperty("tags")
    @Valid
    private List<String> tags = new ArrayList<String>();
    @JsonProperty("title")
    private String title;
    @JsonProperty("vendor_detail")
    private String vendorDetail;
    /**
     * Metadata Schema
     * <p>
     * Metadata about creation and changes to records, provided by the server (client should not provide)
     *
     */
    @JsonProperty("metadata")
    @JsonPropertyDescription("Metadata about creation and changes to records, provided by the server (client should not provide)")
    @Valid
    private Metadata metadata;

    /**
     *
     * (Required)
     *
     */
    @JsonProperty("id")
    public String getId() {
        return id;
    }

    /**
     *
     * (Required)
     *
     */
    @JsonProperty("id")
    public void setId(String id) {
        this.id = id;
    }

    public Poline withId(String id) {
        this.id = id;
        return this;
    }

    @JsonProperty("edition")
    public String getEdition() {
        return edition;
    }

    @JsonProperty("edition")
    public void setEdition(String edition) {
        this.edition = edition;
    }

    public Poline withEdition(String edition) {
        this.edition = edition;
        return this;
    }

    @JsonProperty("checkin_items")
    public Boolean getCheckinItems() {
        return checkinItems;
    }

    @JsonProperty("checkin_items")
    public void setCheckinItems(Boolean checkinItems) {
        this.checkinItems = checkinItems;
    }

    public Poline withCheckinItems(Boolean checkinItems) {
        this.checkinItems = checkinItems;
        return this;
    }

    @JsonProperty("instance_id")
    public String getInstanceId() {
        return instanceId;
    }

    @JsonProperty("instance_id")
    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public Poline withInstanceId(String instanceId) {
        this.instanceId = instanceId;
        return this;
    }

    @JsonProperty("agreement_id")
    public String getAgreementId() {
        return agreementId;
    }

    @JsonProperty("agreement_id")
    public void setAgreementId(String agreementId) {
        this.agreementId = agreementId;
    }

    public Poline withAgreementId(String agreementId) {
        this.agreementId = agreementId;
        return this;
    }

    @JsonProperty("purchase_order_id")
    public String getPurchaseOrderId() {
        return purchaseOrderId;
    }

    @JsonProperty("purchase_order_id")
    public void setPurchaseOrderId(String purchaseOrderId) {
        this.purchaseOrderId = purchaseOrderId;
    }

    public Poline withPurchaseOrderId(String purchaseOrderId) {
        this.purchaseOrderId = purchaseOrderId;
        return this;
    }

    @JsonProperty("acquisition_method")
    public String getAcquisitionMethod() {
        return acquisitionMethod;
    }

    @JsonProperty("acquisition_method")
    public void setAcquisitionMethod(String acquisitionMethod) {
        this.acquisitionMethod = acquisitionMethod;
    }

    public Poline withAcquisitionMethod(String acquisitionMethod) {
        this.acquisitionMethod = acquisitionMethod;
        return this;
    }

    @JsonProperty("adjustment")
    public String getAdjustment() {
        return adjustment;
    }

    @JsonProperty("adjustment")
    public void setAdjustment(String adjustment) {
        this.adjustment = adjustment;
    }

    public Poline withAdjustment(String adjustment) {
        this.adjustment = adjustment;
        return this;
    }

    @JsonProperty("alerts")
    public List<Object> getAlerts() {
        return alerts;
    }

    @JsonProperty("alerts")
    public void setAlerts(List<Object> alerts) {
        this.alerts = alerts;
    }

    public Poline withAlerts(List<Object> alerts) {
        this.alerts = alerts;
        return this;
    }

    @JsonProperty("cancellation_restriction")
    public String getCancellationRestriction() {
        return cancellationRestriction;
    }

    @JsonProperty("cancellation_restriction")
    public void setCancellationRestriction(String cancellationRestriction) {
        this.cancellationRestriction = cancellationRestriction;
    }

    public Poline withCancellationRestriction(String cancellationRestriction) {
        this.cancellationRestriction = cancellationRestriction;
        return this;
    }

    @JsonProperty("cancellation_restriction_note")
    public String getCancellationRestrictionNote() {
        return cancellationRestrictionNote;
    }

    @JsonProperty("cancellation_restriction_note")
    public void setCancellationRestrictionNote(String cancellationRestrictionNote) {
        this.cancellationRestrictionNote = cancellationRestrictionNote;
    }

    public Poline withCancellationRestrictionNote(String cancellationRestrictionNote) {
        this.cancellationRestrictionNote = cancellationRestrictionNote;
        return this;
    }

    @JsonProperty("claims")
    public List<String> getClaims() {
        return claims;
    }

    @JsonProperty("claims")
    public void setClaims(List<String> claims) {
        this.claims = claims;
    }

    public Poline withClaims(List<String> claims) {
        this.claims = claims;
        return this;
    }

    @JsonProperty("collection")
    public Boolean getCollection() {
        return collection;
    }

    @JsonProperty("collection")
    public void setCollection(Boolean collection) {
        this.collection = collection;
    }

    public Poline withCollection(Boolean collection) {
        this.collection = collection;
        return this;
    }

    @JsonProperty("cost")
    public String getCost() {
        return cost;
    }

    @JsonProperty("cost")
    public void setCost(String cost) {
        this.cost = cost;
    }

    public Poline withCost(String cost) {
        this.cost = cost;
        return this;
    }

    @JsonProperty("count")
    public Integer getCount() {
    return count;
  }

    @JsonProperty("count")
    public void setCost(Integer count) {
    this.count = count;
  }

    public Poline withCost(Integer count) {
      this.count = count;
      return this;
   }

   @JsonProperty("description")
    public String getDescription() {
        return description;
    }

    @JsonProperty("description")
    public void setDescription(String description) {
        this.description = description;
    }

    public Poline withDescription(String description) {
        this.description = description;
        return this;
    }

    @JsonProperty("details")
    public String getDetails() {
        return details;
    }

    @JsonProperty("details")
    public void setDetails(String details) {
        this.details = details;
    }

    public Poline withDetails(String details) {
        this.details = details;
        return this;
    }

    @JsonProperty("donor")
    public String getDonor() {
        return donor;
    }

    @JsonProperty("donor")
    public void setDonor(String donor) {
        this.donor = donor;
    }

    public Poline withDonor(String donor) {
        this.donor = donor;
        return this;
    }

    @JsonProperty("eresource")
    public String getEresource() {
        return eresource;
    }

    @JsonProperty("eresource")
    public void setEresource(String eresource) {
        this.eresource = eresource;
    }

    public Poline withEresource(String eresource) {
        this.eresource = eresource;
        return this;
    }

    @JsonProperty("fund_distribution")
    public List<String> getFundDistribution() {
        return fundDistribution;
    }

    @JsonProperty("fund_distribution")
    public void setFundDistribution(List<String> fundDistribution) {
        this.fundDistribution = fundDistribution;
    }

    public Poline withFundDistribution(List<String> fundDistribution) {
        this.fundDistribution = fundDistribution;
        return this;
    }

    @JsonProperty("location")
    public String getLocation() {
        return location;
    }

    @JsonProperty("location")
    public void setLocation(String location) {
        this.location = location;
    }

    public Poline withLocation(String location) {
        this.location = location;
        return this;
    }

    @JsonProperty("order_format")
    public String getOrderFormat() {
        return orderFormat;
    }

    @JsonProperty("order_format")
    public void setOrderFormat(String orderFormat) {
        this.orderFormat = orderFormat;
    }

    public Poline withOrderFormat(String orderFormat) {
        this.orderFormat = orderFormat;
        return this;
    }

    @JsonProperty("owner")
    public String getOwner() {
        return owner;
    }

    @JsonProperty("owner")
    public void setOwner(String owner) {
        this.owner = owner;
    }

    public Poline withOwner(String owner) {
        this.owner = owner;
        return this;
    }

    @JsonProperty("payment_status")
    public String getPaymentStatus() {
        return paymentStatus;
    }

    @JsonProperty("payment_status")
    public void setPaymentStatus(String paymentStatus) {
        this.paymentStatus = paymentStatus;
    }

    public Poline withPaymentStatus(String paymentStatus) {
        this.paymentStatus = paymentStatus;
        return this;
    }

    @JsonProperty("physical")
    public String getPhysical() {
        return physical;
    }

    @JsonProperty("physical")
    public void setPhysical(String physical) {
        this.physical = physical;
    }

    public Poline withPhysical(String physical) {
        this.physical = physical;
        return this;
    }

    @JsonProperty("po_line_description")
    public String getPoLineDescription() {
        return poLineDescription;
    }

    @JsonProperty("po_line_description")
    public void setPoLineDescription(String poLineDescription) {
        this.poLineDescription = poLineDescription;
    }

    public Poline withPoLineDescription(String poLineDescription) {
        this.poLineDescription = poLineDescription;
        return this;
    }

    @JsonProperty("po_line_number")
    public String getPoLineNumber() {
        return poLineNumber;
    }

    @JsonProperty("po_line_number")
    public void setPoLineNumber(String poLineNumber) {
        this.poLineNumber = poLineNumber;
    }

    public Poline withPoLineNumber(String poLineNumber) {
        this.poLineNumber = poLineNumber;
        return this;
    }

    @JsonProperty("po_line_workflow_status")
    public String getPoLineWorkflowStatus() {
        return poLineWorkflowStatus;
    }

    @JsonProperty("po_line_workflow_status")
    public void setPoLineWorkflowStatus(String poLineWorkflowStatus) {
        this.poLineWorkflowStatus = poLineWorkflowStatus;
    }

    public Poline withPoLineWorkflowStatus(String poLineWorkflowStatus) {
        this.poLineWorkflowStatus = poLineWorkflowStatus;
        return this;
    }

    @JsonProperty("publication_date")
    public String getPublicationDate() {
        return publicationDate;
    }

    @JsonProperty("publication_date")
    public void setPublicationDate(String publicationDate) {
        this.publicationDate = publicationDate;
    }

    public Poline withPublicationDate(String publicationDate) {
        this.publicationDate = publicationDate;
        return this;
    }

    @JsonProperty("publisher")
    public String getPublisher() {
        return publisher;
    }

    @JsonProperty("publisher")
    public void setPublisher(String publisher) {
        this.publisher = publisher;
    }

    public Poline withPublisher(String publisher) {
        this.publisher = publisher;
        return this;
    }

    @JsonProperty("receipt_date")
    public String getReceiptDate() {
        return receiptDate;
    }

    @JsonProperty("receipt_date")
    public void setReceiptDate(String receiptDate) {
        this.receiptDate = receiptDate;
    }

    public Poline withReceiptDate(String receiptDate) {
        this.receiptDate = receiptDate;
        return this;
    }

    @JsonProperty("receipt_status")
    public String getReceiptStatus() {
        return receiptStatus;
    }

    @JsonProperty("receipt_status")
    public void setReceiptStatus(String receiptStatus) {
        this.receiptStatus = receiptStatus;
    }

    public Poline withReceiptStatus(String receiptStatus) {
        this.receiptStatus = receiptStatus;
        return this;
    }

    @JsonProperty("reporting_codes")
    public List<String> getReportingCodes() {
        return reportingCodes;
    }

    @JsonProperty("reporting_codes")
    public void setReportingCodes(List<String> reportingCodes) {
        this.reportingCodes = reportingCodes;
    }

    public Poline withReportingCodes(List<String> reportingCodes) {
        this.reportingCodes = reportingCodes;
        return this;
    }

    @JsonProperty("requester")
    public String getRequester() {
        return requester;
    }

    @JsonProperty("requester")
    public void setRequester(String requester) {
        this.requester = requester;
    }

    public Poline withRequester(String requester) {
        this.requester = requester;
        return this;
    }

    @JsonProperty("rush")
    public Boolean getRush() {
        return rush;
    }

    @JsonProperty("rush")
    public void setRush(Boolean rush) {
        this.rush = rush;
    }

    public Poline withRush(Boolean rush) {
        this.rush = rush;
        return this;
    }

    @JsonProperty("selector")
    public String getSelector() {
        return selector;
    }

    @JsonProperty("selector")
    public void setSelector(String selector) {
        this.selector = selector;
    }

    public Poline withSelector(String selector) {
        this.selector = selector;
        return this;
    }

    @JsonProperty("source")
    public String getSource() {
        return source;
    }

    @JsonProperty("source")
    public void setSource(String source) {
        this.source = source;
    }

    public Poline withSource(String source) {
        this.source = source;
        return this;
    }

    @JsonProperty("tags")
    public List<String> getTags() {
        return tags;
    }

    @JsonProperty("tags")
    public void setTags(List<String> tags) {
        this.tags = tags;
    }

    public Poline withTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    @JsonProperty("title")
    public String getTitle() {
        return title;
    }

    @JsonProperty("title")
    public void setTitle(String title) {
        this.title = title;
    }

    public Poline withTitle(String title) {
        this.title = title;
        return this;
    }

    @JsonProperty("vendor_detail")
    public String getVendorDetail() {
        return vendorDetail;
    }

    @JsonProperty("vendor_detail")
    public void setVendorDetail(String vendorDetail) {
        this.vendorDetail = vendorDetail;
    }

    public Poline withVendorDetail(String vendorDetail) {
        this.vendorDetail = vendorDetail;
        return this;
    }

    /**
     * Metadata Schema
     * <p>
     * Metadata about creation and changes to records, provided by the server (client should not provide)
     *
     */
    @JsonProperty("metadata")
    public Metadata getMetadata() {
        return metadata;
    }

    /**
     * Metadata Schema
     * <p>
     * Metadata about creation and changes to records, provided by the server (client should not provide)
     *
     */
    @JsonProperty("metadata")
    public void setMetadata(Metadata metadata) {
        this.metadata = metadata;
    }

    public Poline withMetadata(Metadata metadata) {
        this.metadata = metadata;
        return this;
    }

}
