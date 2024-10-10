/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.vertx.ext.auth.mtls.impl;

import java.security.cert.Certificate;
import java.security.cert.CertificateExpiredException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.naming.InvalidNameException;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

import io.vertx.ext.auth.authentication.CertificateCredentials;
import io.vertx.ext.auth.authentication.CredentialValidationException;
import io.vertx.ext.auth.mtls.CertificateValidator;

/**
 * {@link CertificateValidator} implementation that can be used for validating certificates.
 */
public class CertificateValidatorImpl implements CertificateValidator
{
    private final Set<String> trustedCNs;
    private final String trustedIssuerOrganization;
    private final String trustedIssuerOrganizationUnit;
    private final String trustedIssuerCountry;

    public CertificateValidatorImpl()
    {
        this.trustedCNs = Collections.emptySet();
        this.trustedIssuerOrganization = null;
        this.trustedIssuerOrganizationUnit = null;
        this.trustedIssuerCountry = null;
    }

    public CertificateValidatorImpl(Set<String> trustedCNs,
                                    String trustedIssuerOrganization,
                                    String trustedIssuerOrganizationUnit,
                                    String trustedIssuerCountry)
    {
        this.trustedCNs = Collections.unmodifiableSet(trustedCNs);
        this.trustedIssuerOrganization = trustedIssuerOrganization;
        this.trustedIssuerOrganizationUnit = trustedIssuerOrganizationUnit;
        this.trustedIssuerCountry = trustedIssuerCountry;
    }

    @Override
    public boolean isValidCertificate(CertificateCredentials credentials)
    {
        credentials.checkValid();
        Certificate certificate = credentials.certificateChain().get(0);
        if (certificate instanceof X509Certificate)
        {
            X509Certificate castedCert = (X509Certificate) certificate;
            if (!isValidIssuer(castedCert))
            {
                return false;
            }

            try
            {
                castedCert.checkValidity();
                return true;
            }
            catch (CertificateExpiredException e)
            {
                throw new CredentialValidationException("Expired certificates shared for authentication");
            }
            catch (Exception e)
            {
                return false;
            }
        }
        return false;
    }

    private boolean isValidIssuer(X509Certificate certificate)
    {
        List<Attributes> issuerAttrs;
        try
        {
            issuerAttrs = getAttributes(new LdapName(certificate.getIssuerDN().getName()));

            boolean trustedCN
            = trustedCNs.isEmpty() || trustedCNs.contains(getAttribute(issuerAttrs, "CN"));
            boolean trustedOrganization
            = trustedIssuerOrganization == null || getAttribute(issuerAttrs, "O").equalsIgnoreCase(trustedIssuerOrganization);
            boolean trustedOrganizationUnit
            = trustedIssuerOrganizationUnit == null || getAttribute(issuerAttrs, "OU").equalsIgnoreCase(trustedIssuerOrganizationUnit);
            boolean trustedCountry
            = trustedIssuerCountry == null || getAttribute(issuerAttrs, "C").equalsIgnoreCase(trustedIssuerCountry);

            return trustedCN && trustedOrganization && trustedOrganizationUnit && trustedCountry;
        }
        catch (InvalidNameException e)
        {
            throw new CredentialValidationException("Certificate issuer could not be verified");
        }
        catch (NamingException e)
        {
            throw new CredentialValidationException("Error validating certificate issuer");
        }
    }

    private List<Attributes> getAttributes(LdapName ldapName)
    {
        List<Rdn> rdns = ldapName.getRdns();
        List<Attributes> attributes = new ArrayList<>(rdns.size());
        for (int i = 0; i < rdns.size(); ++i)
        {
            attributes.add(rdns.get(i).toAttributes());
        }
        return attributes;
    }

    private String getAttribute(List<Attributes> attributesList, String attributeName) throws NamingException
    {
        for (int i = 0; i < attributesList.size(); ++i)
        {
            Attributes attributes = attributesList.get(i);
            Attribute value = attributes.get(attributeName);
            if (value != null)
            {
                return value.get().toString();
            }
        }
        throw new CredentialValidationException(String.format("Expected attribute %s not found", attributeName));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    /**
     * Builder that provides default implementation of {@link CertificateValidator}.
     */
    public static class Builder
    {
        Set<String> trustedCNs = Collections.emptySet();
        String trustedIssuerOrganization;
        String trustedIssuerOrganizationUnit;
        String trustedIssuerCountry;

        public Builder trustedCNs(Set<String> trustedCNs)
        {
            this.trustedCNs = trustedCNs;
            return this;
        }

        public Builder trustedIssuerOrganization(String trustedIssuerOrganization)
        {
            this.trustedIssuerOrganization = trustedIssuerOrganization;
            return this;
        }

        public Builder trustedIssuerOrganizationUnit(String trustedIssuerOrganizationUnit)
        {
            this.trustedIssuerOrganizationUnit = trustedIssuerOrganizationUnit;
            return this;
        }

        public Builder trustedIssuerCountry(String trustedIssuerCountry)
        {
            this.trustedIssuerCountry = trustedIssuerCountry;
            return this;
        }

        public CertificateValidatorImpl build()
        {
            return new CertificateValidatorImpl(trustedCNs, trustedIssuerOrganization, trustedIssuerOrganizationUnit, trustedIssuerCountry);
        }
    }
}
