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

package io.vertx.ext.auth.mtls.utils;

import java.io.IOException;
import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

import javax.security.auth.x500.X500Principal;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.BasicConstraints;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

/**
 * For building certificates for unit testing with specified details such as issuer, validity date etc.
 */
public class CertificateBuilder
{
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();
    private static final String ALGORITHM = "EC";
    private static final ECGenParameterSpec ALGORITHM_PARAMETER_SPEC = new ECGenParameterSpec("secp256r1");
    private static final String SIGNATURE_ALGORITHM = "SHA256WITHECDSA";
    private static final GeneralName[] EMPTY_SAN = {};

    private String alias;
    private BigInteger serial = new BigInteger(159, SECURE_RANDOM);
    private Date notBefore = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
    private Date notAfter = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
    private X500Name subject;
    private final List<GeneralName> subjectAlternativeNames = new ArrayList<>();
    private boolean isCertificateAuthority;

    public CertificateBuilder alias(String alias)
    {
        this.alias = Objects.requireNonNull(alias);
        return this;
    }

    public CertificateBuilder serial(BigInteger serial)
    {
        this.serial = serial;
        return this;
    }

    public CertificateBuilder notBefore(Date notBefore)
    {
        this.notBefore = Date.from(notBefore.toInstant());
        return this;
    }

    public CertificateBuilder notAfter(Date notAfter)
    {
        this.notAfter = Date.from(notBefore.toInstant());
        return this;
    }

    public CertificateBuilder subject(String subject)
    {
        this.subject = new X500Name(subject);
        return this;
    }

    public CertificateBuilder addSanUriName(String uri)
    {
        subjectAlternativeNames.add(new GeneralName(GeneralName.uniformResourceIdentifier, uri));
        return this;
    }

    public CertificateBuilder isCertificateAuthority(boolean isCertificateAuthority)
    {
        this.isCertificateAuthority = isCertificateAuthority;
        return this;
    }

    public CertificateBuilder addSanDnsName(String dnsName)
    {
        subjectAlternativeNames.add(new GeneralName(GeneralName.dNSName, dnsName));
        return this;
    }

    public CertificateBuilder addSanIpAddress(String name)
    {
        this.subjectAlternativeNames.add(new GeneralName(GeneralName.iPAddress, name));
        return this;
    }

    public static CertificateBuilder builder()
    {
        return new CertificateBuilder();
    }

    public CertificateBundle buildSelfSigned() throws Exception
    {
        KeyPair keyPair = generateKeyPair();

        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(subject, serial, notBefore, notAfter, subject, keyPair.getPublic());
        addExtensions(builder);

        ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(keyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        X509Certificate root = new JcaX509CertificateConverter().getCertificate(holder);
        return new CertificateBundle(SIGNATURE_ALGORITHM, new X509Certificate[]{ root }, root, keyPair, alias);
    }

    public CertificateBundle buildIssuedBy(CertificateBundle issuer)
    throws GeneralSecurityException, IOException, OperatorCreationException
    {
        String issuerSignAlgorithm = issuer.signatureAlgorithm();
        return buildIssuedBy(issuer, issuerSignAlgorithm);
    }

    public CertificateBundle buildIssuedBy(CertificateBundle issuer, String issuerSignAlgorithm)
    throws GeneralSecurityException, IOException, OperatorCreationException
    {
        KeyPair keyPair = generateKeyPair();

        X500Principal issuerPrincipal = issuer.certificate().getSubjectX500Principal();
        X500Name issuerName = X500Name.getInstance(issuerPrincipal.getEncoded());
        JcaX509v3CertificateBuilder builder = createCertBuilder(issuerName, subject, keyPair);

        addExtensions(builder);

        PrivateKey issuerPrivateKey = issuer.keyPair().getPrivate();
        if (issuerPrivateKey == null)
        {
            throw new IllegalArgumentException("Cannot sign certificate with issuer that does not have a private key.");
        }
        ContentSigner signer = new JcaContentSignerBuilder(issuerSignAlgorithm).build(issuerPrivateKey);
        X509CertificateHolder holder = builder.build(signer);
        X509Certificate cert = new JcaX509CertificateConverter().getCertificate(holder);
        X509Certificate[] issuerPath = issuer.certificatePath();
        X509Certificate[] path = new X509Certificate[issuerPath.length + 1];
        path[0] = cert;
        System.arraycopy(issuerPath, 0, path, 1, issuerPath.length);
        return new CertificateBundle(SIGNATURE_ALGORITHM, path, issuer.rootCertificate(), keyPair, alias);
    }

    private KeyPair generateKeyPair() throws GeneralSecurityException
    {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
        keyGen.initialize(ALGORITHM_PARAMETER_SPEC, SECURE_RANDOM);
        return keyGen.generateKeyPair();
    }

    private void addExtensions(JcaX509v3CertificateBuilder builder) throws IOException
    {
        if (isCertificateAuthority)
        {
            builder.addExtension(Extension.basicConstraints, true, new BasicConstraints(true));
        }

        boolean criticality = false;
        if (!subjectAlternativeNames.isEmpty())
        {
            builder.addExtension(Extension.subjectAlternativeName, criticality,
                                 new GeneralNames(subjectAlternativeNames.toArray(EMPTY_SAN)));
        }
    }

    private JcaX509v3CertificateBuilder createCertBuilder(X500Name issuer, X500Name subject, KeyPair keyPair)
    {
        BigInteger serial = this.serial != null ? this.serial : new BigInteger(159, SECURE_RANDOM);
        PublicKey pubKey = keyPair.getPublic();
        return new JcaX509v3CertificateBuilder(issuer, serial, notBefore, notAfter, subject, pubKey);
    }
}
