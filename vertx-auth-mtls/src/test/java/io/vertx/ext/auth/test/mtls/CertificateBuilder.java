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

package io.vertx.ext.auth.test.mtls;

import java.math.BigInteger;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.Extension;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralNames;
import org.bouncycastle.cert.CertIOException;
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

    private BigInteger serial = new BigInteger(159, SECURE_RANDOM);
    private Date notBefore = Date.from(Instant.now().minus(1, ChronoUnit.DAYS));
    private Date notAfter = Date.from(Instant.now().plus(1, ChronoUnit.DAYS));
    private X500Name issuerName;
    private List<GeneralName> subjectAlternativeNames = new ArrayList<>();

    public CertificateBuilder serial(BigInteger serial)
    {
        this.serial = serial;
        return this;
    }

    public CertificateBuilder notBefore(Date notBefore)
    {
        this.notBefore = notBefore;
        return this;
    }

    public CertificateBuilder notAfter(Date notAfter)
    {
        this.notAfter = notAfter;
        return this;
    }

    public CertificateBuilder issuerName(String issuer)
    {
        this.issuerName = new X500Name(issuer);
        return this;
    }

    public CertificateBuilder addSanUriName(String uri)
    {
        subjectAlternativeNames.add(new GeneralName(GeneralName.uniformResourceIdentifier, uri));
        return this;
    }

    public static CertificateBuilder builder()
    {
        return new CertificateBuilder();
    }

    public X509Certificate buildSelfSigned() throws GeneralSecurityException, CertIOException, OperatorCreationException
    {
        KeyPair keyPair = generateKeyPair();

        JcaX509v3CertificateBuilder builder = new JcaX509v3CertificateBuilder(issuerName, serial, notBefore, notAfter, issuerName, keyPair.getPublic());
        builder.addExtension(Extension.subjectAlternativeName, false, new GeneralNames(subjectAlternativeNames.toArray(EMPTY_SAN)));

        ContentSigner signer = new JcaContentSignerBuilder(SIGNATURE_ALGORITHM).build(keyPair.getPrivate());
        X509CertificateHolder holder = builder.build(signer);
        return new JcaX509CertificateConverter().getCertificate(holder);
    }

    private KeyPair generateKeyPair() throws GeneralSecurityException
    {
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance(ALGORITHM);
        keyGen.initialize(ALGORITHM_PARAMETER_SPEC, SECURE_RANDOM);
        return keyGen.generateKeyPair();
    }
}
