/*
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2020 Dmitry Degrave
 *  Copyright (c) 2020 Garvan Institute of Medical Research
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package org.annii.entity

object Chromosome extends Enumeration {
  val Chr1  = Value("1")
  val Chr2  = Value("2")
  val Chr3  = Value("3")
  val Chr4  = Value("4")
  val Chr5  = Value("5")
  val Chr6  = Value("6")
  val Chr7  = Value("7")
  val Chr8  = Value("8")
  val Chr9  = Value("9")
  val Chr10 = Value("10")
  val Chr11 = Value("11")
  val Chr12 = Value("12")
  val Chr13 = Value("13")
  val Chr14 = Value("14")
  val Chr15 = Value("15")
  val Chr16 = Value("16")
  val Chr17 = Value("17")
  val Chr18 = Value("18")
  val Chr19 = Value("19")
  val Chr20 = Value("20")
  val Chr21 = Value("21")
  val Chr22 = Value("22")
  val ChrX  = Value("X")
  val ChrY  = Value("Y")
  val ChrMT = Value("MT")

  def withNameOrNone(s: String): Option[Value] = values.find(_.toString == s)
}