/*
 * Copyright (c) Sebastian Ertel 2010. All Rights Reserved.
 * 
 * This source code is licensed under the terms described in the associated LICENSE.TXT file.
 */

package ohua.runtime.engine.exceptions;

import java.util.function.Supplier;

public abstract class Assertion
{
  public static final boolean ENABLE_INVARIANT_CHECKS = false;

  public static void invariant(boolean invariant)
  {
    if(ENABLE_INVARIANT_CHECKS) {
      if (!invariant) {
        System.out.println("INVARIANT BROKEN");
        System.err.println("INVARIANT BROKEN");
        InvariantBroken invariantBroken = new InvariantBroken();
        invariantBroken.printStackTrace();
        throw invariantBroken;
      }
    }
  }

  public static void invariant(boolean invariant, String whatBroke)
  {
    if(ENABLE_INVARIANT_CHECKS) {
      if (!invariant) {
        System.out.println("INVARIANT BROKEN");
        System.err.println("INVARIANT BROKEN");
        InvariantBroken invariantBroken = new InvariantBroken(whatBroke);
        invariantBroken.printStackTrace();
        throw invariantBroken;
      }
    }
  }

  public static void invariant(boolean invariant, Supplier<String> s)
  {
    if(ENABLE_INVARIANT_CHECKS) {
      if (!invariant) {
        System.out.println("INVARIANT BROKEN");
        System.err.println("INVARIANT BROKEN");
        InvariantBroken invariantBroken = new InvariantBroken(s.get());
        invariantBroken.printStackTrace();
        throw invariantBroken;
      }
    }
  }

  public static void impossible(Exception cause)
  {
      System.out.println("INVARIANT BROKEN");
      System.err.println("INVARIANT BROKEN");
      InvariantBroken invariantBroken = new InvariantBroken(cause);
      invariantBroken.printStackTrace();
      throw invariantBroken;
  }

  public static void impossible(String whatBroke)
  {
    System.out.println("INVARIANT BROKEN");
    System.err.println("INVARIANT BROKEN");
    InvariantBroken invariantBroken = new InvariantBroken(whatBroke);
    invariantBroken.printStackTrace();
    throw invariantBroken;
  }

  public static void impossible()
  {
    System.out.println("INVARIANT BROKEN");
    System.err.println("INVARIANT BROKEN");
    InvariantBroken invariantBroken = new InvariantBroken();
    invariantBroken.printStackTrace();
    throw invariantBroken;
  }

}
