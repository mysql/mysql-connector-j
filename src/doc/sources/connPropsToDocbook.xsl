<?xml version="1.0" encoding="iso-8859-1"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

<!-- 

     This stylesheet converts the output of com.mysql.jdbc.util.PropertiesDocGenerator
     to a DocBook table for inclusion in the product manual.

-->

<xsl:template match="ConnectionProperties">
	<xsl:apply-templates select = "PropertyCategory" />
</xsl:template>

<xsl:template match="PropertyCategory">
	<xsl:element name="formalpara">
		<xsl:element name="title">
			<xsl:value-of select="@name" />
		</xsl:element>
		<xsl:element name="para">
			<xsl:element name="informaltable">
				<xsl:element name="textobject">
					<xsl:element name="phrase">
						<xsl:text>This table lists Connector/J </xsl:text>
						<xsl:value-of select="@name" />
						<xsl:text> connection properties.</xsl:text>
					</xsl:element>
				</xsl:element>
				<xsl:element name="tgroup">
					<xsl:attribute name="cols">4</xsl:attribute>
					<xsl:element name="colspec">
						<xsl:attribute name="colwidth">30*</xsl:attribute>
						<xsl:attribute name="colname">cj_propstbl_prop_name</xsl:attribute>
					</xsl:element>
					<xsl:element name="colspec">
						<xsl:attribute name="colwidth">50*</xsl:attribute>
						<xsl:attribute name="colname">cj_propstbl_prop_defn</xsl:attribute>
					</xsl:element>
					<xsl:element name="colspec">
						<xsl:attribute name="colwidth">10*</xsl:attribute>
						<xsl:attribute name="colname">cj_propstbl_default</xsl:attribute>
					</xsl:element>
					<xsl:element name="colspec">
						<xsl:attribute name="colwidth">10*</xsl:attribute>
						<xsl:attribute name="colname">cj_propstbl_since_version</xsl:attribute>
					</xsl:element>
					<xsl:element name="tbody">
						<xsl:element name="row">
							<xsl:element name="entry">
								<xsl:element name="emphasis">
									<xsl:attribute name="role">bold</xsl:attribute>
									Property Name
								</xsl:element>
							</xsl:element>
							<xsl:element name="entry">
								<xsl:element name="emphasis">
									<xsl:attribute name="role">bold</xsl:attribute>
								Definition	
								</xsl:element>
							</xsl:element>
							<xsl:element name="entry">
								<xsl:element name="emphasis">
									<xsl:attribute name="role">bold</xsl:attribute>
								Default Value	
								</xsl:element>
							</xsl:element>
							<xsl:element name="entry">
								<xsl:element name="emphasis">
									<xsl:attribute name="role">bold</xsl:attribute>
								Since Version	
								</xsl:element>
							</xsl:element>
						</xsl:element>

						<xsl:apply-templates select = "./Property"/>
					</xsl:element>
				</xsl:element> <!-- end tgroup -->
			</xsl:element> <!-- end informaltable -->
		</xsl:element> <!-- end para -->
	</xsl:element> <!-- end formalpara -->
	
</xsl:template>

<xsl:template match="Property">
	<xsl:element name="row">
		<xsl:element name="entry">
			<xsl:value-of select="@name" />
		</xsl:element>
		<xsl:element name="entry">
			<xsl:value-of select="." />
		</xsl:element>
		<xsl:element name="entry">
			<xsl:value-of select="@default" />
		</xsl:element>
		<xsl:element name="entry">
			<xsl:value-of select="@since" />
		</xsl:element>
	</xsl:element>
</xsl:template>

</xsl:stylesheet>
