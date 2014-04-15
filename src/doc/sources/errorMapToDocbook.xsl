<?xml version="1.0" encoding="iso-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<!-- 

     This stylesheet converts the output of com.mysql.jdbc.util.ErrorMappingsDocGenerator
     to a DocBook table for inclusion in the product manual.

-->

<xsl:template match="ErrorMappings">
	<xsl:element name="table">
		<xsl:element name="title">Mapping of MySQL Error Numbers to SQLStates</xsl:element>
    	<xsl:element name="tgroup"><xsl:attribute name="cols">4</xsl:attribute>
                <xsl:element name="colspec"><xsl:attribute name="colwidth">20*</xsl:attribute></xsl:element>
                <xsl:element name="colspec"><xsl:attribute name="colwidth">40*</xsl:attribute></xsl:element>
                <xsl:element name="colspec"><xsl:attribute name="colwidth">20*</xsl:attribute></xsl:element>
                <xsl:element name="colspec"><xsl:attribute name="colwidth">20*</xsl:attribute></xsl:element>
    		<xsl:element name="thead">
    			<xsl:element name="row">
    				<xsl:element name="entry">MySQL Error Number</xsl:element>
    				<xsl:element name="entry">MySQL Error Name</xsl:element>
    				<xsl:element name="entry">Legacy (X/Open) SQLState</xsl:element>
    				<xsl:element name="entry">SQL Standard SQLState</xsl:element>
				</xsl:element> <!-- row -->
			</xsl:element> <!-- thead -->
			<xsl:element name="tbody">
				<xsl:apply-templates select = "ErrorMapping" />
			</xsl:element> <!-- tbody -->
		</xsl:element> <!-- tgroup -->
	</xsl:element>
</xsl:template>

<xsl:template match="ErrorMapping">
	<xsl:element name="row">
		<xsl:element name="entry">
			<xsl:value-of select="@mysqlErrorNumber" />
		</xsl:element>
		<xsl:element name="entry">
			<xsl:value-of select="@mysqlErrorName" />
		</xsl:element>
		<xsl:element name="entry">
			<xsl:value-of select="@legacySqlState" />
		</xsl:element>
		<xsl:element name="entry">
			<xsl:value-of select="@sql92SqlState" />
		</xsl:element>
	</xsl:element>
</xsl:template>

</xsl:stylesheet>