package com.mysql.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * @author mmatthew
 *
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates.
 * To enable and disable the creation of type comments go to
 * Window>Preferences>Java>Code Generation.
 */
public class Password {

    private static final int SHA1_HASH_SIZE = 20;
    private static final char PVERSION41_CHAR = '*';
    
    /*
        Create key from old password to decode scramble
        Used in 4.1 authentication with passwords stored old way

        SYNOPSIS
            create_key_from_old_password()
            passwd    IN  Password used for key generation
            key       OUT Created 20 bytes key

        RETURN
            None
    */

    public static byte[] createKeyFromOldPassword(String passwd) throws NoSuchAlgorithmException {
        
        /* At first hash password to the string stored in password */
        passwd = makeScrambledPassword(passwd);
        /* Now convert it to the salt form */
        short salt = getSaltFromPassword(passwd);
        /* Finally get hash and bin password from salt */
        
        return getBinaryPassword(bytesToHex(shortToBytes(salt), 4), false);
    }
    
    public static String makeScrambledPassword(String password) throws NoSuchAlgorithmException {
        StringBuffer scrambledPassword = new StringBuffer();
        
        scrambledPassword.append(PVERSION41_CHAR); /* New passwords have version prefix */
        /* Random returns number from 0 to 1 so this would be good salt generation.*/
        short saltShort = ((short)(Math.random() * 65535 + 1));
        System.out.println(saltShort);
        byte[] salt = shortToBytes(saltShort);
        
        /* Use only 2 first bytes from it */
        
        String saltStr = bytesToHex(salt, 2);
        scrambledPassword.append(saltStr);
       
        /* First hasing is done without salt */
        byte[] digest = passwordHashStage1(password);
        /* Second stage is done with salt */
        byte[] finalDigest = passwordHashStage2(digest, saltStr.getBytes());
        /* Print resulting hash as hex*/
        String hashHex = bytesToHex(finalDigest, finalDigest.length);
        scrambledPassword.append(hashHex);
        
        return scrambledPassword.toString();
    }
    
    /*
        Stage one password hashing.
        Used in MySQL 4.1 password handling

        SYNOPSIS
            password_hash_stage1()
            to       OUT    Store stage one hash to this location
            password IN     Plain text password to build hash

        RETURN
            none
    */

    public static byte[] passwordHashStage1(String password) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        StringBuffer cleansedPassword = new StringBuffer();
     
        int passwordLength = password.length();
           
        for (int i = 0; i < passwordLength; i++) {
            char c = password.charAt(i);
            
            if (c == ' ' || c == '\t') {
                continue;/* skip space in password */
            }
            
            cleansedPassword.append(c);
        }
        
        return md.digest(cleansedPassword.toString().getBytes());
    }

    /*
        Stage two password hashing.
        Used in MySQL 4.1 password handling

        SYNOPSIS
            password_hash_stage2()
            to       INOUT  Use this as stage one hash and store stage two hash here
            salt     IN     Salt used for stage two hashing

        RETURN
            none
    */

    public static byte[] passwordHashStage2(byte[] hashedPassword, byte[] salt) throws NoSuchAlgorithmException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
  
        
        // hash 4 bytes of salt
        md.update(salt,0, 4);
        
        md.update(hashedPassword,0, SHA1_HASH_SIZE);
        
        return md.digest();
    }
    
    /*
        Encrypt/Decrypt function used for password encryption in authentication
        Simple XOR is used here but it is OK as we crypt random strings

        SYNOPSIS
        password_crypt()
            from     IN     Data for encryption
            to       OUT    Encrypt data to the buffer (may be the same)
            password IN     Password used for encryption (same length)
            length   IN     Length of data to encrypt

        RETURN
            none
    */

    public static void passwordCrypt(byte[] from, byte[] to, byte[] password,int length)
    {
        int pos = 0;    

        while (pos < from.length) {
            to[pos]= (byte)(from[pos] ^ password[pos]);
            pos++;
        }
    }

    
    private static String bytesToHex(byte[] val, int numBytes) {
        
        StringBuffer hexValues = new StringBuffer();
        
        for (int i = 0; i < val.length && i < numBytes; i++) {
        
            String hexVal = Integer.toHexString(
                                        (int) val[i] & 0xff);

            if (hexVal.length() == 1) {
                hexVal = "0" + hexVal;
            }
            
            hexValues.append(hexVal);
        }
        
        return hexValues.toString();
    }
    
    
    /**
     * Returns hex value for given char
     */
    private static int charVal(char c) {
 
        return (int) (c >= '0' && c <= '9' ? c-'0' :
                 c >= 'A' && c <= 'Z' ? c-'A'+10 :
                 c-'a'+10);
    }

    private static byte[] shortToBytes(short val) {
        try {
            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            DataOutputStream dOut = new DataOutputStream(bOut);
        
            dOut.writeShort(val);
            dOut.flush();
            dOut.close();
        
            return bOut.toByteArray();
        } catch (IOException ioEx) {
            // do nothing, should never be thrown from
            // ByteArrayOutputStream
        }
        
        return null;
    }
    
    private static short getSaltFromPassword(String password) {
        if (password == null || password.length() == 0) {
            return 0;
        }
 
        if (password.charAt(0) == PVERSION41_CHAR) {
            // new password
            String saltInHex = password.substring(1, 5);
            
            System.out.println(saltInHex);
            
            int val = 0;
        
            for (int i = 0 ; i < 4 ; i++) {
                val=(val << 4) + charVal(saltInHex.charAt(i));
            }
        
            return (short) val;
            
        } else {
            // old password
        }
        
        return 0;
    }
    
    public static byte[] getBinaryPassword(String salt, boolean usingNewPasswords) throws NoSuchAlgorithmException {
    
        int val = 0;
        
        byte[] binaryPassword = new byte[SHA1_HASH_SIZE]; /* Binary password loop pointer */

        if (usingNewPasswords) /* New password version assumed */
        {
    
            int pos = 0;
            
            for (int i = 0; i < 4; i++) /* Iterate over these elements*/
            {
                val = salt.charAt(i);
                for (int t=3; t>=0; t--) {
                    binaryPassword[pos++] = (byte) (val & 255);
                    val >>= 8; /* Scroll 8 bits to get next part*/
                }
            }
            
            return binaryPassword;
        } else {
            int pos = 0;
            
            for (int i = 0; i < 2; i++) /* Iterate over these elements*/
            {
                val = salt.charAt(i);
                for (int t=3; t>=0; t--) {
                    binaryPassword[pos++] = (byte) (val % 256);
                    val >>= 8; /* Scroll 8 bits to get next part*/
                }
            }
            
            MessageDigest md = MessageDigest.getInstance("SHA-1");
        
            md.update(binaryPassword,0, 8);
        
            return md.digest();
        }
    }
    
    public static void main(String[] args) throws Exception {
        String password = makeScrambledPassword("test");
        
        System.out.println(password);
        System.out.println(getSaltFromPassword(password));
        System.out.println(new String(getBinaryPassword("9019", false)));
    }
    


}
